import aws_cdk as cdk
from aws_cdk import (
    Stack, Duration,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
)
from constructs import Construct

class WxStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # --- DynamoDB Tables ---
        self.readings_table = dynamodb.Table(
            self, "WxReadings",
            table_name="wx-readings",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        self.stats_table = dynamodb.Table(
            self, "WxDailyStats",
            table_name="wx-daily-stats",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="month_hour", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        self.baselines_table = dynamodb.Table(
            self, "WxBaselines",
            table_name="wx-baselines",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="month", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- IAM policy for Secrets Manager ---
        secrets_policy = iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[
                "arn:aws:secretsmanager:us-east-1:216890068001:secret:ambient-weather/*",
                "arn:aws:secretsmanager:us-east-1:216890068001:secret:weatherkit/*",
            ]
        )

        # --- Lambda factory (closure over self) ---
        def make_lambda(fn_id, handler_module, memory=128, timeout=30, extra_env=None):
            pkg = handler_module.split('.')[0]
            env = {
                "READINGS_TABLE": self.readings_table.table_name,
                "STATS_TABLE": self.stats_table.table_name,
                "BASELINES_TABLE": self.baselines_table.table_name,
                **(extra_env or {}),
            }
            fn = lambda_.Function(
                self, fn_id,
                runtime=lambda_.Runtime.PYTHON_3_12,
                architecture=lambda_.Architecture.ARM_64,
                handler=f"{handler_module}.handler",
                code=lambda_.Code.from_asset(
                    "../lambdas",
                    bundling=cdk.BundlingOptions(
                        image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                        command=[
                            "bash", "-c",
                            f"pip install -r {pkg}/requirements.txt "
                            f"-t /asset-output --platform manylinux2014_aarch64 --only-binary=:all: && "
                            f"cp -r shared {pkg} /asset-output/"
                        ]
                    )
                ),
                memory_size=memory,
                timeout=Duration.seconds(timeout),
                environment=env,
            )
            fn.add_to_role_policy(secrets_policy)
            self.readings_table.grant_read_write_data(fn)
            self.stats_table.grant_read_write_data(fn)
            self.baselines_table.grant_read_write_data(fn)
            return fn

        # --- Lambdas ---
        self.poller_fn = make_lambda("WxPoller", "wx_poller.handler", timeout=30)
        poller_rule = events.Rule(
            self, "WxPollerSchedule",
            schedule=events.Schedule.rate(Duration.minutes(5)),
        )
        poller_rule.add_target(targets.LambdaFunction(self.poller_fn))

        self.api_fn = make_lambda("WxApi", "wx_api.handler", timeout=10)

        self.bootstrap_fn = make_lambda("WxBootstrap", "wx_bootstrap.handler",
                                         memory=256, timeout=300)

        # --- API Gateway HTTP API ---
        http_api = apigwv2.HttpApi(
            self, "WxHttpApi",
            api_name="wx-api",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[apigwv2.CorsHttpMethod.GET],
            ),
        )
        lambda_integration = integrations.HttpLambdaIntegration("WxApiIntegration", self.api_fn)
        http_api.add_routes(path="/current", methods=[apigwv2.HttpMethod.GET], integration=lambda_integration)
        http_api.add_routes(path="/history", methods=[apigwv2.HttpMethod.GET], integration=lambda_integration)

        # --- CloudFront in front of API Gateway ---
        api_origin = origins.HttpOrigin(
            f"{http_api.api_id}.execute-api.us-east-1.amazonaws.com",
            protocol_policy=cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
        )
        api_cache_policy = cloudfront.CachePolicy(
            self, "WxApiCachePolicy",
            default_ttl=Duration.minutes(5),
            min_ttl=Duration.seconds(0),
            max_ttl=Duration.minutes(5),
        )
        self.api_distribution = cloudfront.Distribution(
            self, "WxApiDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=api_origin,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=api_cache_policy,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD,
            ),
        )
        cdk.CfnOutput(self, "ApiDistributionDomain", value=self.api_distribution.distribution_domain_name)

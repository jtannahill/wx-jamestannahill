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

        # --- Nearby stations snapshots (one row per 5-min poll, 30-day TTL) ---
        self.nearby_table = dynamodb.Table(
            self, "WxNearbySnapshots",
            table_name="wx-nearby-snapshots",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="snapshot_at", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
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
        self.poller_fn = make_lambda("WxPoller", "wx_poller.handler", timeout=30,
                                     extra_env={"DASHBOARD_BUCKET": "wx-jamestannahill-dashboard"})
        # Grant poller write access to dashboard bucket (for OG image)
        self.poller_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["s3:PutObject"],
            resources=["arn:aws:s3:::wx-jamestannahill-dashboard/og.png"],
        ))
        poller_rule = events.Rule(
            self, "WxPollerSchedule",
            schedule=events.Schedule.rate(Duration.minutes(5)),
        )
        poller_rule.add_target(targets.LambdaFunction(self.poller_fn))

        self.api_fn = make_lambda("WxApi", "wx_api.handler", timeout=10)

        self.bootstrap_fn = make_lambda("WxBootstrap", "wx_bootstrap.handler",
                                         memory=256, timeout=300)

        # --- Alerts DynamoDB table ---
        self.alerts_table = dynamodb.Table(
            self, "WxAlerts",
            table_name="wx-alerts",
            partition_key=dynamodb.Attribute(name="alert_type", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- Alerter Lambda ---
        self.alerter_fn = make_lambda(
            "WxAlerter", "wx_alerter.handler",
            timeout=30,
            extra_env={
                "ALERTS_TABLE": self.alerts_table.table_name,
                "ALERT_FROM":   "wx@jamestannahill.com",
                "ALERT_TO":     "james@jamestannahill.com",
            }
        )
        self.alerts_table.grant_read_write_data(self.alerter_fn)
        self.alerter_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["ses:SendEmail"],
            resources=["*"],
        ))
        alerter_rule = events.Rule(
            self, "WxAlerterSchedule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
        )
        alerter_rule.add_target(targets.LambdaFunction(self.alerter_fn))

        # --- Forecasts DynamoDB table ---
        self.forecasts_table = dynamodb.Table(
            self, "WxForecasts",
            table_name="wx-forecasts",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- Forecast accuracy DynamoDB table ---
        self.accuracy_table = dynamodb.Table(
            self, "WxForecastAccuracy",
            table_name="wx-forecast-accuracy",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="evaluated_at", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- UHI seasonal DynamoDB table ---
        self.uhi_seasonal_table = dynamodb.Table(
            self, "WxUhiSeasonal",
            table_name="wx-uhi-seasonal",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="month", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- ML models DynamoDB table ---
        self.ml_models_table = dynamodb.Table(
            self, "WxMlModels",
            table_name="wx-ml-models",
            partition_key=dynamodb.Attribute(name="model_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- Forecaster Lambda (memory=512 for the 90-day scan) ---
        self.forecaster_fn = make_lambda(
            "WxForecaster", "wx_forecaster.handler",
            memory=512, timeout=120,
            extra_env={
                "FORECASTS_TABLE": self.forecasts_table.table_name,
                "ACCURACY_TABLE":  self.accuracy_table.table_name,
            },
        )
        self.forecasts_table.grant_read_write_data(self.forecaster_fn)
        self.accuracy_table.grant_read_write_data(self.forecaster_fn)
        forecaster_rule = events.Rule(
            self, "WxForecasterSchedule",
            schedule=events.Schedule.rate(Duration.minutes(30)),
        )
        forecaster_rule.add_target(targets.LambdaFunction(self.forecaster_fn))

        # --- ML Fitter Lambda (weekly, 900s for 90-day scan + training) ---
        self.ml_fitter_fn = make_lambda(
            "WxMlFitter", "wx_ml_fitter.handler",
            memory=512, timeout=900,
            extra_env={"MODELS_TABLE": self.ml_models_table.table_name},
        )
        self.ml_models_table.grant_read_write_data(self.ml_fitter_fn)
        ml_fitter_rule = events.Rule(
            self, "WxMlFitterSchedule",
            schedule=events.Schedule.cron(hour="3", minute="0", week_day="SUN"),
        )
        ml_fitter_rule.add_target(targets.LambdaFunction(self.ml_fitter_fn))

        # Allow the API Lambda to read forecasts, accuracy, UHI seasonal, and ML models
        self.forecasts_table.grant_read_data(self.api_fn)
        self.accuracy_table.grant_read_data(self.api_fn)
        self.uhi_seasonal_table.grant_read_data(self.api_fn)
        self.ml_models_table.grant_read_data(self.api_fn)

        # Pass table names to the API Lambda
        self.api_fn.add_environment("FORECASTS_TABLE",    self.forecasts_table.table_name)
        self.api_fn.add_environment("ACCURACY_TABLE",     self.accuracy_table.table_name)
        self.api_fn.add_environment("UHI_SEASONAL_TABLE", self.uhi_seasonal_table.table_name)
        self.api_fn.add_environment("MODELS_TABLE",       self.ml_models_table.table_name)

        # Allow the poller to write UHI seasonal data
        self.uhi_seasonal_table.grant_read_write_data(self.poller_fn)
        self.poller_fn.add_environment("UHI_SEASONAL_TABLE", self.uhi_seasonal_table.table_name)

        # Poller writes nearby snapshots; API reads them
        self.nearby_table.grant_read_write_data(self.poller_fn)
        self.poller_fn.add_environment("NEARBY_TABLE", self.nearby_table.table_name)
        self.nearby_table.grant_read_data(self.api_fn)
        self.api_fn.add_environment("NEARBY_TABLE", self.nearby_table.table_name)

        # --- Daily summaries DynamoDB table ---
        self.daily_summaries_table = dynamodb.Table(
            self, "WxDailySummaries",
            table_name="wx-daily-summaries",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="date", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- Station records DynamoDB table ---
        self.station_records_table = dynamodb.Table(
            self, "WxStationRecords",
            table_name="wx-station-records",
            partition_key=dynamodb.Attribute(name="station_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="month", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        # --- Summarizer Lambda (daily at 05:00 UTC = midnight ET) ---
        self.summarizer_fn = make_lambda(
            "WxSummarizer", "wx_summarizer.handler",
            memory=256, timeout=300,
            extra_env={"SUMMARIES_TABLE": self.daily_summaries_table.table_name},
        )
        self.daily_summaries_table.grant_read_write_data(self.summarizer_fn)
        summarizer_rule = events.Rule(
            self, "WxSummarizerSchedule",
            schedule=events.Schedule.cron(hour="5", minute="0"),
        )
        summarizer_rule.add_target(targets.LambdaFunction(self.summarizer_fn))

        # --- Records tracker Lambda (weekly, Sunday 02:00 UTC) ---
        self.records_fn = make_lambda(
            "WxRecordsTracker", "wx_records_tracker.handler",
            memory=256, timeout=300,
            extra_env={"RECORDS_TABLE": self.station_records_table.table_name},
        )
        self.station_records_table.grant_read_write_data(self.records_fn)
        records_rule = events.Rule(
            self, "WxRecordsTrackerSchedule",
            schedule=events.Schedule.cron(hour="2", minute="0", week_day="SUN"),
        )
        records_rule.add_target(targets.LambdaFunction(self.records_fn))

        # Allow the API Lambda to read summaries and records
        self.daily_summaries_table.grant_read_data(self.api_fn)
        self.station_records_table.grant_read_data(self.api_fn)
        self.api_fn.add_environment("SUMMARIES_TABLE", self.daily_summaries_table.table_name)
        self.api_fn.add_environment("RECORDS_TABLE",   self.station_records_table.table_name)

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
        http_api.add_routes(path="/current",           methods=[apigwv2.HttpMethod.GET], integration=lambda_integration)
        http_api.add_routes(path="/history",            methods=[apigwv2.HttpMethod.GET], integration=lambda_integration)
        http_api.add_routes(path="/rain-events",        methods=[apigwv2.HttpMethod.GET], integration=lambda_integration)
        http_api.add_routes(path="/daily-summaries",    methods=[apigwv2.HttpMethod.GET], integration=lambda_integration)
        http_api.add_routes(path="/nearby", methods=[apigwv2.HttpMethod.GET], integration=lambda_integration)

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
            query_string_behavior=cloudfront.CacheQueryStringBehavior.all(),
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

        # --- Dashboard S3 + CloudFront ---
        self.dashboard_bucket = s3.Bucket(
            self, "WxDashboardBucket",
            bucket_name="wx-jamestannahill-dashboard",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        oac = cloudfront.S3OriginAccessControl(self, "WxDashboardOAC")
        dashboard_origin = origins.S3BucketOrigin.with_origin_access_control(
            self.dashboard_bucket, origin_access_control=oac
        )

        self.dashboard_distribution = cloudfront.Distribution(
            self, "WxDashboardDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=dashboard_origin,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
            ),
            default_root_object="index.html",
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                )
            ],
        )

        cdk.CfnOutput(self, "DashboardBucketName", value=self.dashboard_bucket.bucket_name)
        cdk.CfnOutput(self, "DashboardDistributionId", value=self.dashboard_distribution.distribution_id)
        cdk.CfnOutput(self, "DashboardDistributionDomain", value=self.dashboard_distribution.distribution_domain_name)

import aws_cdk as cdk
from wx_stack import WxStack

app = cdk.App()
WxStack(app, "WxStack", env=cdk.Environment(account="216890068001", region="us-east-1"))
app.synth()

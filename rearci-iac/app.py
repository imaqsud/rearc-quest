import aws_cdk as cdk
from rearci_iac.rearci_iac_stack import RearcQuestStack

app = cdk.App()
RearcQuestStack(app, "RearcQuestStack")
app.synth()

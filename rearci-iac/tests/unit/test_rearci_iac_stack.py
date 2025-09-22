import aws_cdk as core
import aws_cdk.assertions as assertions

from rearci_iac.rearci_iac_stack import RearciIacStack

# example tests. To run these tests, uncomment this file along with the example
# resource in rearci_iac/rearci_iac_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = RearciIacStack(app, "rearci-iac")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })

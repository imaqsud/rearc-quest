from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_s3_notifications as s3n,
    aws_iam as iam,
)
from constructs import Construct

class RearcQuestStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        bucket = s3.Bucket(self, "RearcQuestBucket",
            bucket_name="rearcio-quest-bucket",
        )

        queue = sqs.Queue(self, "IngestQueue")

        ingest_fn = _lambda.Function(self, "IngestLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="ingest.handler",
            code=_lambda.Code.from_asset("lambda/ingest"),
            timeout=Duration.minutes(5),
            memory_size=512
        )
        # grant permissions
        bucket.grant_read_write(ingest_fn)
        queue.grant_send_messages(ingest_fn)
        # Add permission to list queues
        ingest_fn.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["sqs:ListQueues"],
                resources=["*"]
            )
        )

        rule = events.Rule(self, "DailyTrigger", schedule=events.Schedule.cron(minute="50", hour="10"))
        rule.add_target(targets.LambdaFunction(ingest_fn))

        # Lambda for analytics
        analytics_fn = _lambda.Function(self, "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="analytics.handler",
            code=_lambda.Code.from_asset("lambda/analytics"),
            timeout=Duration.minutes(3),
            memory_size=256
        )
        bucket.grant_read_write(analytics_fn)
        queue.grant_consume_messages(analytics_fn)

        # S3 -> SQS notification when JSON is created
        # Note: CDK requires that SQS and bucket are in same region/stack for direct notification
        bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(queue),
                                      s3.NotificationKeyFilter(prefix="population/", suffix=".json"))
        
        # SQS event source mapping to trigger analytics Lambda
        analytics_fn.add_event_source_mapping(
            "AnalyticsEventSource",
            event_source_arn=queue.queue_arn,
            batch_size=1
        )

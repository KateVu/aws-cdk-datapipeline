from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    Duration,  # Import Duration directly from aws_cdk
)
from constructs import Construct


class StepFunction(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        region: str,
        account: str,
        env_name: str,
        ingestion_glue_job_name: str,
        transformation_glue_job_name: str,
        glue_crawler_staging_name: str,
        glue_crawler_transformation_name: str,
        sns_topic_arn: str,
        input_bucket_name: str,  # Use the existing bucket name
        file_names: list,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # Define the ingestion Glue job task
        ingestion_glue_task = tasks.GlueStartJobRun(
            self,
            "IngestionGlueJob",
            glue_job_name=ingestion_glue_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # Wait for job completion
            arguments=sfn.TaskInput.from_object(
                {
                    "--file_path": sfn.JsonPath.string_at(
                        "$.file_path"
                    ),  # Pass file_path from input
                }
            ),
        )

        # Define the transformation Glue job task
        transformation_glue_task = tasks.GlueStartJobRun(
            self,
            "TransformationGlueJob",
            glue_job_name=transformation_glue_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # Wait for job completion
        )

        # Define the Glue crawler staging task
        glue_crawler_staging_task = tasks.CallAwsService(
            self,
            "GlueCrawlerStagingTask",
            service="glue",
            action="startCrawler",
            parameters={"Name": glue_crawler_staging_name},
            iam_resources=[
                f"arn:aws:glue:{region}:{account}:crawler/{glue_crawler_staging_name}"
            ],
        )

        # Define the Glue crawler transformation task
        glue_crawler_transformation_task = tasks.CallAwsService(
            self,
            "GlueCrawlerTransformationTask",
            service="glue",
            action="startCrawler",
            parameters={"Name": glue_crawler_transformation_name},
            iam_resources=[
                f"arn:aws:glue:{region}:{account}:crawler/{glue_crawler_transformation_name}"
            ],
        )

        # Define the SNS publish task
        sns_publish_task = tasks.CallAwsService(
            self,
            "SNSPublishTask",
            service="sns",
            action="publish",
            parameters={
                "TopicArn": sns_topic_arn,
                "Message": f"Step Function {env_name}-DataPipelineStateMachine has completed successfully.",
            },
            iam_resources=[f"arn:aws:sns:{region}:{account}:*"],
        )

        # Wait state for the staging crawler
        wait_staging = sfn.Wait(
            self,
            "WaitForStagingCrawler",
            time=sfn.WaitTime.duration(Duration.seconds(30)),
        )

        # GetCrawler state for the staging crawler
        get_staging_crawler = tasks.CallAwsService(
            self,
            "GetStagingCrawlerState",
            service="glue",
            action="getCrawler",
            parameters={"Name": glue_crawler_staging_name},
            iam_resources=[
                f"arn:aws:glue:{region}:{account}:crawler/{glue_crawler_staging_name}"
            ],
        )

        # Success and fail states for the staging crawler
        staging_success = sfn.Succeed(self, "StagingCrawlerSuccess")
        staging_failed = sfn.Fail(self, "StagingCrawlerFailed")

        # Choice state for the staging crawler
        staging_crawler_complete = sfn.Choice(self, "StagingCrawlerComplete")
        staging_crawler_complete.when(
            sfn.Condition.string_equals("$.Crawler.State", "READY"), staging_success
        )
        staging_crawler_complete.when(
            sfn.Condition.string_equals("$.Crawler.State", "FAILED"), staging_failed
        )
        staging_crawler_complete.otherwise(wait_staging)

        # Wait state for the transformation crawler
        wait_transformation = sfn.Wait(
            self,
            "WaitForTransformationCrawler",
            time=sfn.WaitTime.duration(Duration.seconds(30)),
        )

        # GetCrawler state for the transformation crawler
        get_transformation_crawler = tasks.CallAwsService(
            self,
            "GetTransformationCrawlerState",
            service="glue",
            action="getCrawler",
            parameters={"Name": glue_crawler_transformation_name},
            iam_resources=[
                f"arn:aws:glue:{region}:{account}:crawler/{glue_crawler_transformation_name}"
            ],
        )

        # Success and fail states for the transformation crawler
        transformation_success = sfn.Succeed(self, "TransformationCrawlerSuccess")
        transformation_failed = sfn.Fail(self, "TransformationCrawlerFailed")

        # Choice state for the transformation crawler
        transformation_crawler_complete = sfn.Choice(
            self, "TransformationCrawlerComplete"
        )
        transformation_crawler_complete.when(
            sfn.Condition.string_equals("$.Crawler.State", "READY"),
            transformation_success,
        )
        transformation_crawler_complete.when(
            sfn.Condition.string_equals("$.Crawler.State", "FAILED"),
            transformation_failed,
        )
        transformation_crawler_complete.otherwise(wait_transformation)

        # Run transformation Glue job and Glue crawler staging in parallel
        parallel_tasks = sfn.Parallel(self, "ParallelTasks")
        parallel_tasks.branch(
            transformation_glue_task.next(glue_crawler_transformation_task)
            .next(wait_transformation)
            .next(get_transformation_crawler)
            .next(transformation_crawler_complete)
        )
        parallel_tasks.branch(
            glue_crawler_staging_task.next(wait_staging)
            .next(get_staging_crawler)
            .next(staging_crawler_complete)
        )

        # Chain the ingestion Glue job, parallel tasks, transformation crawler, and SNS publish task
        definition = ingestion_glue_task.next(parallel_tasks).next(sns_publish_task)

        # Create the Step Function
        self.state_machine = sfn.StateMachine(
            self,
            f"{env_name}-DataPipelineStateMachine",
            definition=definition,
        )

        # Reference the existing S3 bucket
        input_bucket = s3.Bucket.from_bucket_name(
            self,
            "ExistingInputBucket",
            bucket_name=input_bucket_name,
        )

        # Create a Lambda function to trigger the Step Function
        trigger_lambda = _lambda.Function(
            self,
            "TriggerLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="trigger.handler",
            code=_lambda.Code.from_asset("./scripts/lambda/"),  # Path to Lambda code
            environment={
                "STEP_FUNCTION_ARN": self.state_machine.state_machine_arn,
                "REGION": region,
                "ACCOUNT": account,
                "BUCKET_NAME": input_bucket.bucket_name,
                "FILE_NAMES": ",".join(
                    file_names
                ),  # Convert list to comma-separated string
            },
        )

        # Grant permissions to the Lambda function
        input_bucket.grant_read(trigger_lambda)
        self.state_machine.grant_start_execution(trigger_lambda)

        # Add S3 event notification to invoke the Lambda function only for files in the env_name folder
        input_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.LambdaDestination(trigger_lambda),
            s3.NotificationKeyFilter(
                prefix=f"{env_name}/"
            ),  # Trigger only for files in env_name folder
        )

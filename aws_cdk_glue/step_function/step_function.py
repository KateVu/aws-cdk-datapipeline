from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
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
        glue_crawler_transformation_name: str,  # Added transformation crawler name
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # Define the ingestion Glue job task
        ingestion_glue_task = tasks.GlueStartJobRun(
            self,
            "IngestionGlueJob",
            glue_job_name=ingestion_glue_job_name,
        )

        # Define the transformation Glue job task
        transformation_glue_task = tasks.GlueStartJobRun(
            self,
            "TransformationGlueJob",
            glue_job_name=transformation_glue_job_name,
        )

        # Define the Glue crawler staging task
        glue_crawler_staging_task = tasks.CallAwsService(
            self,
            "GlueCrawlerStagingTask",
            service="glue",
            action="startCrawler",
            parameters={"Name": glue_crawler_staging_name},
            iam_resources=[f"arn:aws:glue:{region}:{account}:crawler/{glue_crawler_staging_name}"],
        )

        # Define the Glue crawler transformation task
        glue_crawler_transformation_task = tasks.CallAwsService(
            self,
            "GlueCrawlerTransformationTask",
            service="glue",
            action="startCrawler",
            parameters={"Name": glue_crawler_transformation_name},
            iam_resources=[f"arn:aws:glue:{region}:{account}:crawler/{glue_crawler_transformation_name}"],
        )

        # Run transformation Glue job and Glue crawler staging in parallel
        parallel_tasks = sfn.Parallel(self, "ParallelTasks")
        parallel_tasks.branch(transformation_glue_task.next(glue_crawler_transformation_task))
        parallel_tasks.branch(glue_crawler_staging_task)

        # Chain the ingestion Glue job, parallel tasks, and transformation crawler
        definition = ingestion_glue_task.next(parallel_tasks)

        # Create the Step Function
        self.state_machine = sfn.StateMachine(
            self,
            f"{env_name}-DataPipelineStateMachine",
            definition=definition,
        )
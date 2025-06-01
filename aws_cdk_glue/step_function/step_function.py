from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class StepFunction(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_name: str,
        ingestion_glue_job_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # Define the Glue job task for Step Functions
        ingestion_glue_job_task = tasks.GlueStartJobRun(
            self,
            "GlueJobTask",
            glue_job_name=ingestion_glue_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # Define the Step Function workflow
        definition = ingestion_glue_job_task

        # Create the Step Function
        self.state_machine = sfn.StateMachine(
            self,
            f"DataPipelineStateMachine-{env_name}",
            definition=definition,
        )

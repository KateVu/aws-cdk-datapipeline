from aws_cdk import Stack, CfnOutput
from constructs import Construct
from aws_cdk_glue.glue.ingestion import GlueIngestion
from aws_cdk_glue.step_function.step_function import StepFunction


class DataPipelineStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env_name: str,
        account_config: dict,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create the Glue ingestion construct
        glue_ingestion = GlueIngestion(
            self,
            "GlueIngestion",
            env_name=env_name,
            input_bucket=account_config["ingestion"]["input_bucket"],
            output_bucket=account_config["ingestion"]["output_bucket"],
            error_bucket=account_config["ingestion"]["error_bucket"],
            file_names=account_config["ingestion"]["file_names"],
        )

        # Create the Step Function
        step_function = StepFunction(
            self,
            "DataPipelineStepFunction",
            env_name=env_name,
            ingestion_glue_job_name=glue_ingestion.glue_job.name,
        )

        # Output the Step Function ARN
        CfnOutput(
            self,
            "StepFunctionArn",
            value=step_function.state_machine.state_machine_arn,
            description="The ARN of the Step Function for the data pipeline",
        )

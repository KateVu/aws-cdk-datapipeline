from aws_cdk import Stack
from constructs import Construct
from aws_cdk_glue.glue.glue_contruct import GlueContruct
from aws_cdk_glue.step_function.step_function import StepFunction
from aws_cdk_glue.athena.athena_table import AthenaTable


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
        glue_ingestion = GlueContruct(
            self,
            "GlueIngestion",
            env_name=env_name,
            input_bucket=account_config["ingestion"]["input_bucket"],
            output_bucket=account_config["ingestion"]["output_bucket"],
            error_bucket=account_config["ingestion"]["error_bucket"],
            file_names=account_config["ingestion"]["file_names"],
            script_file_path="../../scripts/glue/ingestion.py",  # Path to your Glue script
            glue_job_prefix="IngestionJob",
        )

        glue_transformation = GlueContruct(
            self,
            "GlueTransformation",
            env_name=env_name,
            input_bucket=account_config["transformation"]["input_bucket"],
            output_bucket=account_config["transformation"]["output_bucket"],
            error_bucket=account_config["transformation"]["error_bucket"],
            file_names=account_config["transformation"]["file_names"],
            script_file_path="../../scripts/glue/transformation.py",  # Path to your Glue script
            glue_job_prefix="TransformationJob",
        )

        # Create the Step Function
        step_function = StepFunction(
            self,
            "DataPipelineStepFunction",
            env_name=env_name,
            ingestion_glue_job_name=glue_ingestion.glue_job.name,
        )

        # Create the Athena table
        AthenaTable(
            self,
            "AthenaTable",
            env_name=env_name,
            staging_bucket=account_config["ingestion"]["output_bucket"],
            account_id=account_config["account_id"],
            region=self.region,
            staging_file_names=account_config["ingestion"]["file_names"],
            transformation_bucket=account_config["transformation"]["output_bucket"],
            transformation_file_names=account_config["transformation"]["file_names"],
        )



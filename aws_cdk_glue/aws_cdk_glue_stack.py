from aws_cdk import Stack
from constructs import Construct
from aws_cdk_glue.glue.ingestion import GlueIngestionConstruct


class DataPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env_name: str, account_config: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create the Glue ingestion construct
        GlueIngestionConstruct(
            self,
            "GlueIngestion",
            env_name=env_name,
            input_bucket=account_config["ingestion"]["input_bucket"],
            output_bucket=account_config["ingestion"]["output_bucket"],
            error_bucket=account_config["ingestion"]["error_bucket"],
            file_names=account_config["ingestion"]["file_names"],
        )
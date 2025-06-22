from aws_cdk import Stack
from aws_cdk import aws_sns as sns
from constructs import Construct
from aws_cdk_glue.glue.glue_contruct import GlueContruct
from aws_cdk_glue.step_function.step_function import StepFunction
from aws_cdk_glue.athena.athena_table import AthenaTable
from aws_cdk_glue.utils.utils import add_output  # Import the utility method


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
        # Create an SNS topic
        sns_topic = sns.Topic(
            self,
            "DataPipelineSNSTopic",
            display_name=f"{env_name}-DataPipelineTopic",
            topic_name=f"{env_name}-DataPipelineTopic",
        )

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
            sns_topic_arn=sns_topic.topic_arn,  # Pass the SNS topic ARN
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
            sns_topic_arn=sns_topic.topic_arn,  # Pass the SNS topic ARN
        )

        # Create the Athena table
        athena_table = AthenaTable(
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

        # Create the Step Function
        step_function = StepFunction(
            self,
            "StepFunction",
            region=self.region,
            account=account_config["account_id"],
            env_name=env_name,
            ingestion_glue_job_name=glue_ingestion.glue_job.name,
            transformation_glue_job_name=glue_transformation.glue_job.name,
            glue_crawler_staging_name=athena_table.glue_crawler_staging.name,
            glue_crawler_transformation_name=athena_table.glue_crawler_transformation.name,  # Added transformation crawler name
        )




        # Output the SNS topic ARN
        add_output(self, "SNSTopicARN", sns_topic.topic_arn)

        # Output Glue job names
        add_output(self, "GlueIngestionJobName", glue_ingestion.glue_job.name)
        add_output(self, "GlueTransformationJobName", glue_transformation.glue_job.name)



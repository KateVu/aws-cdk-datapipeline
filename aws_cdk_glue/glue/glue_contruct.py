from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_s3_assets as s3_assets,
)
from constructs import Construct
import os.path as path


class GlueContruct(Construct):

    def __init__(self, scope: Construct, id: str, env_name: str, input_bucket: str, output_bucket: str, error_bucket: str, file_names: list, script_file_path: str, glue_job_prefix: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        # self.env_name = env_name
        # self.input_bucket = input_bucket
        # self.output_bucket = output_bucket
        # self.error_bucket = error_bucket
        # self.file_names = file_names
        # Define an IAM role for the Glue job
        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        # Add permissions to read from the input bucket and write to the output bucket
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    f"arn:aws:s3:::{input_bucket}/{env_name}/*",
                    f"arn:aws:s3:::{input_bucket}/{env_name}",
                ],
            )
        )

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
                resources=[
                    f"arn:aws:s3:::{output_bucket}/{env_name}/*",
                    f"arn:aws:s3:::{output_bucket}/{env_name}",
                ],
            )
        )

        # Upload the Glue script to an S3 bucket using an S3 asset
        glue_script_asset = s3_assets.Asset(
            self,
            "GlueScriptAsset",
            path=path.join(
                path.dirname(__file__),script_file_path
            ),  # Replace with the local path to your script
        )

        glue_script_asset.grant_read(glue_role)

        # Define the Glue job
        self.glue_job = glue.CfnJob(
            self,
            glue_job_prefix + env_name,
            name=f"{glue_job_prefix}-{env_name}",
            role=glue_role.role_arn,
            command={
                "name": "glueetl",
                "scriptLocation": glue_script_asset.s3_object_url,
                "pythonVersion": "3",
            },
            default_arguments={
                "--env_name": env_name,
                "--input_bucket": input_bucket,
                "--output_bucket": output_bucket,
                "--error_bucket": error_bucket,
                "--file_names": ",".join(file_names),
                "--file_path": "test",  # Assuming files are in a 'data' folder for now
            },
            max_retries=1,
            timeout=10,
            glue_version="5.0",
        )

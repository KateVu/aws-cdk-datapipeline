#!/usr/bin/env python3
import os
from aws_cdk import App, Environment, Tags

from aws_cdk_glue.aws_cdk_datapipeline_stack import DataPipelineStack
from aws_cdk_glue.utils.utils import get_config_account

# Environment variables
config_folder = "../config/"
account_file_name = "aws_account.yaml"

env_name = os.getenv("ENVIRONMENT_NAME", "kate")
account_name = os.getenv("ACCOUNT_NAME", "sandpit2")
region = os.getenv("REGION", "ap-southeast-2")

# Get account configuration
account_config = get_config_account(account_name, file_name=account_file_name, config_folder=config_folder)
account_id = account_config["account_id"]

# Define the environment for the stack
environment = Environment(account=account_id, region=region)

app = App()

# Create the stack
pipeline_stack = DataPipelineStack(
    app,
    construct_id="DataPipelineStack",
    stack_name=f"DataPipelineStack-{env_name}",
    env_name=env_name,
    account_config=account_config,
    env=environment,
)

# Add tags to the EC2 instance
Tags.of(pipeline_stack).add("createdby", "KateVu")
Tags.of(pipeline_stack).add("createdvia", "AWS-CDK")
Tags.of(pipeline_stack).add("environment", env_name)
Tags.of(pipeline_stack).add("repo", "https://github.com/KateVu/aws-cdk-glue")

app.synth()

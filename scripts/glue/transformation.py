import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def notify_sns(sns_client, topic_arn, message):
    """
    Send a notification to an SNS topic.

    :param sns_client: Boto3 SNS client
    :param topic_arn: ARN of the SNS topic
    :param message: Message to send
    """
    try:
        sns_client.publish(TopicArn=topic_arn, Message=message)
        logger.info(f"Notification sent to SNS topic: {topic_arn}")
    except ClientError as e:
        logger.error(f"Failed to send notification to SNS topic: {e}")


def process_file(spark, input_bucket, output_bucket, error_bucket, env_name, file_name, sns_client, sns_topic_arn):
    """
    Process individual files based on their type.

    :param spark: SparkSession object
    :param input_bucket: Name of the input S3 bucket
    :param output_bucket: Name of the output S3 bucket
    :param error_bucket: Name of the error S3 bucket
    :param env_name: Environment name (e.g., dev, prod)
    :param file_name: Name of the file to process
    :param sns_client: Boto3 SNS client
    :param sns_topic_arn: ARN of the SNS topic
    """
    input_path = f"s3://{input_bucket}/{env_name}/staging_{file_name}"
    output_path = f"s3://{output_bucket}/{env_name}/transformation_{file_name}"
    error_path = f"s3://{error_bucket}/{env_name}/{file_name}_error.log"

    logger.info(f"Processing file: {file_name}")
    logger.info(f"Reading data from: {input_path}")

    try:
        # Read Parquet file from S3
        df = spark.read.parquet(input_path)

        if file_name == "customer_data":
            # Copy customer_data as is
            logger.info("Copying customer_data without transformation.")
        elif file_name == "transaction_data":
            # Rename column 'amounttt' to 'amount' for transaction_data
            logger.info("Renaming column 'amounttt' to 'amount' for transaction_data.")
            df = df.withColumnRenamed("amounttt", "amount")
        else:
            logger.warning(f"Unknown file type: {file_name}. Skipping transformation.")

        # Write the processed data to S3 in Parquet format
        logger.info(f"Writing processed data to: {output_path}")
        df.write.parquet(output_path, mode="overwrite")

        logger.info(f"Successfully processed file: {file_name}")
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        logger.info(f"Copying original file to error bucket: {error_path}")
        try:
            s3_client = boto3.client("s3")
            s3_client.copy_object(
                Bucket=error_bucket,
                CopySource={"Bucket": input_bucket, "Key": f"{env_name}/staging_{file_name}"},
                Key=f"{env_name}/{file_name}_error.log",
            )
            logger.info(f"Successfully copied file {file_name} to error bucket.")
        except ClientError as copy_error:
            logger.error(f"Failed to copy file {file_name} to error bucket: {copy_error}")

        # Notify SNS about the error
        error_message = f"Error processing file {file_name} in environment {env_name}. Original file copied to error bucket."
        notify_sns(sns_client, sns_topic_arn, error_message)


def main():
    # Get arguments passed to the Glue job
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "env_name",
            "input_bucket",
            "output_bucket",
            "error_bucket",
            "file_names",
            "sns_topic_arn",  # Added SNS topic ARN argument
        ],
    )

    # Extract arguments
    env_name = args["env_name"]
    input_bucket = args["input_bucket"]
    output_bucket = args["output_bucket"]
    error_bucket = args["error_bucket"]
    file_names = args["file_names"].split(",")  # Comma-separated list of file names
    sns_topic_arn = args["sns_topic_arn"]

    # Initialize Spark session
    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

    # Initialize SNS client
    sns_client = boto3.client("sns")

    # Loop through files and process them
    for file_name in file_names:
        process_file(spark, input_bucket, output_bucket, error_bucket, env_name, file_name, sns_client, sns_topic_arn)

    # Stop the Spark session
    spark.stop()
    logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from awsglue.utils import getResolvedOptions
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_files_exist(s3_client, bucket, env_name, file_path, file_names):
    """
    Check if the specified files exist in the given S3 bucket.

    :param s3_client: Boto3 S3 client
    :param bucket: Name of the S3 bucket
    :param file_names: List of file names to check
    """
    for file_name in file_names:
        try:
            s3_client.head_object(Bucket=bucket, Key=f"{file_path}/{file_name}")
            logger.info(f"File {file_path}/{file_name} exists in bucket {bucket}.")
        except s3_client.exceptions.ClientError as e:
            logger.error(
                f"File {file_path}/{file_name} does not exist in bucket {bucket}: {e} Exiting."
            )
            sys.exit(1)


def delete_directory_in_s3(s3_client, bucket, directory_path):
    """
    Delete all files in the specified directory in the S3 bucket.

    :param s3_client: Boto3 S3 client
    :param bucket: Name of the S3 bucket
    :param directory_path: Path of the directory to delete
    """
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=directory_path)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
                logger.info(f"Deleted file: {obj['Key']} from bucket {bucket}")
        else:
            logger.info(f"No files found in {directory_path} to delete.")
    except Exception as e:
        logger.error(f"Error deleting directory {directory_path}: {e}")
        sys.exit(1)


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
        sys.exit(1)


def process_file(
    spark,
    s3_client,
    sns_client,
    sns_topic_arn,
    input_bucket,
    output_bucket,
    error_bucket,
    file_path,
    file_name,
    env_name,
    current_time,
):
    """
    Process a single file: read from S3, transform, and write to S3.

    :param spark: SparkSession object
    :param s3_client: Boto3 S3 client
    :param sns_client: Boto3 SNS client
    :param sns_topic_arn: ARN of the SNS topic
    :param input_bucket: Name of the input S3 bucket
    :param output_bucket: Name of the output S3 bucket
    :param error_bucket: Name of the error S3 bucket
    :param file_name: Name of the file to process
    :param env_name: Environment name (e.g., dev, prod)
    """
    input_s3_path = f"s3://{input_bucket}/{file_path}/{file_name}"
    output_s3_path = (
        f"s3://{output_bucket}/{env_name}/staging_{file_name.split('.')[0]}/"
    )
    error_s3_path = f"s3://{error_bucket}/{env_name}/error_{file_name}"

    logger.info(f"Processing file: {file_name}")
    logger.info(f"Reading from: {input_s3_path}")

    try:
        # Get the current UTC time for ingestion start
        ingestion_start_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

        # Read CSV file from S3
        df = spark.read.csv(input_s3_path, header=True, inferSchema=True)
        logger.info(f"Finish reading file from s3")
        if not df.columns:
            raise RuntimeError(
                "The DataFrame is empty. Cannot proceed with processing."
            )

        # Add ingestion_start_time column to the DataFrame
        df = df.withColumn("ingestion_start_time", lit(ingestion_start_time))

        # Get the current UTC time for ingestion finish
        ingestion_finish_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Add ingestion_finish_time column to the DataFrame
        df = df.withColumn("ingestion_finish_time", lit(ingestion_finish_time))

        # Write the DataFrame to S3 in Parquet format
        logger.info(f"Writing to: {output_s3_path}")
        df.write.parquet(output_s3_path, mode="overwrite")
        logger.info(f"Successfully processed and saved file: {file_name}")

    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        logger.info(f"Copying original file to error bucket: {error_s3_path}")
        try:
            s3_client.copy_object(
                Bucket=error_bucket,
                CopySource={"Bucket": input_bucket, "Key": f"{file_path}/{file_name}"},
                Key=f"{env_name}/error_{file_name}",
            )
            logger.info(f"Successfully copied file {file_name} to error bucket.")
        except ClientError as copy_error:
            logger.error(
                f"Failed to copy file {file_name} to error bucket: {copy_error}"
            )

        # Notify SNS about the error
        error_message = f"Error processing file {file_name} in environment {env_name}. Original file copied to error bucket."
        notify_sns(sns_client, sns_topic_arn, error_message)


def main():
    # Get arguments passed to the Glue job
    args = getResolvedOptions(
        sys.argv,
        [
            "env_name",
            "input_bucket",
            "output_bucket",
            "error_bucket",
            "file_path",
            "file_names",
            "sns_topic_arn",  # Added SNS topic ARN argument
            "JOB_NAME",
        ],
    )

    # Extract input and output S3 paths from arguments
    input_bucket = args["input_bucket"]
    output_bucket = args["output_bucket"]
    error_bucket = args["error_bucket"]
    file_path = args["file_path"]
    file_names = args["file_names"].split(
        ","
    )  # Expecting a comma-separated list of file names
    env_name = args["env_name"]
    sns_topic_arn = args["sns_topic_arn"]  # Extract SNS topic ARN

    # Initialize S3 and SNS clients
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # Check if files exist in the input bucket
    check_files_exist(s3_client, input_bucket, env_name, file_path, file_names)

    # Delete the entire directory in the output bucket before processing files
    output_directory_path = f"{env_name}/"
    delete_directory_in_s3(s3_client, output_bucket, output_directory_path)

    # Delete the error bucket folder before processing files
    error_directory_path = f"{env_name}/"
    delete_directory_in_s3(s3_client, error_bucket, error_directory_path)

    # Initialize Spark session
    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

    # Process each file
    current_time = datetime.utcnow()
    for file_name in file_names:
        process_file(
            spark,
            s3_client,
            sns_client,
            sns_topic_arn,
            input_bucket,
            output_bucket,
            error_bucket,
            file_path,
            file_name,
            env_name,
            current_time,
        )

    # Stop the Spark session
    spark.stop()
    logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

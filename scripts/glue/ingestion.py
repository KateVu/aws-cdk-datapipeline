import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from awsglue.utils import getResolvedOptions
import boto3

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
            s3_client.head_object(Bucket=bucket, Key=f"{env_name}/{file_path}/{file_name}")
            logger.info(f"File {env_name}/{file_path}/{file_name} exists in bucket {bucket}.")
        except s3_client.exceptions.ClientError as e:
            logger.error(
                f"File {env_name}/{file_path}/{file_name} does not exist in bucket {bucket}: {e} Exiting."
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

def process_file(spark, input_bucket, output_bucket, file_path, file_name, env_name, current_time):
    """
    Process a single file: read from S3, transform, and write to S3.

    :param spark: SparkSession object
    :param input_bucket: Name of the input S3 bucket
    :param output_bucket: Name of the output S3 bucket
    :param file_name: Name of the file to process
    :param env_name: Environment name (e.g., dev, prod)
    """
    # Get the current UTC time for folder structure
    
    year = current_time.strftime("%Y")
    month = current_time.strftime("%m")
    date = current_time.strftime("%d")
    time = current_time.strftime("%H-%M-%S")

    input_s3_path = f"s3://{input_bucket}/{env_name}/{file_path}/{file_name}"
    # output_s3_path = f"s3://{output_bucket}/{env_name}/{file_path}/{year}/{month}/{date}/{time}/{file_name.split('.')[0]}/"  # Save output in a folder with year/month/date/time structure
    output_s3_path = f"s3://{output_bucket}/{env_name}/staging_{file_name.split('.')[0]}/"

    logger.info(f"Processing file: {file_name}")
    logger.info(f"Reading from: {input_s3_path}")

    # Get the current UTC time for ingestion start
    ingestion_start_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    # Read CSV file from S3
    df = spark.read.csv(input_s3_path, header=True, inferSchema=True)
    logger.info(f"Finish reading file from s3")
    # Add ingestion_start_time column to the DataFrame
    df = df.withColumn(
        "ingestion_start_time", lit(ingestion_start_time)
    )

    # Perform any transformations (optional)
    # Example: Filter rows where column 'value' is greater than 100
    # df = df.filter(df["value"] > 100)

    # Get the current UTC time for ingestion finish
    ingestion_finish_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Add ingestion_finish_time column to the DataFrame
    df = df.withColumn(
        "ingestion_finish_time", lit(ingestion_finish_time)
    )

    # Write the DataFrame to S3 in Parquet format
    logger.info(f"Writing to: {output_s3_path}")    
    df.write.parquet(output_s3_path, mode="overwrite")
    logger.info(f"Successfully processed and saved file: {file_name}")


def main():
    # Get arguments passed to the Glue job
    args = getResolvedOptions(
        sys.argv,
        [
            "env_name",
            "input_bucket",
            "output_bucket",
            "file_path",
            "file_names",
            "JOB_NAME",
        ],
    )

    # Extract input and output S3 paths from arguments
    input_bucket = args["input_bucket"]
    output_bucket = args["output_bucket"]
    file_path = args["file_path"]
    file_names = args["file_names"].split(
        ","
    )  # Expecting a comma-separated list of file names
    env_name = args["env_name"]

    # Initialize S3 client
    s3_client = boto3.client("s3")

    # Check if files exist in the input bucket
    check_files_exist(s3_client, input_bucket, env_name, file_path, file_names)

    # Delete the entire directory in the output bucket before processing files
    output_directory_path = f"{env_name}/"
    delete_directory_in_s3(s3_client, output_bucket, output_directory_path)

    # Initialize Spark session
    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

    # Process each file
    current_time = datetime.utcnow()    
    for file_name in file_names:
        process_file(spark, input_bucket, output_bucket, file_path, file_name, env_name, current_time)

    # Stop the Spark session
    spark.stop()
    logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
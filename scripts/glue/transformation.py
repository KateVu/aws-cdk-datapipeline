import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, lit

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_file(spark, input_bucket, output_bucket, error_bucket, env_name, file_name):
    """
    Process individual files based on their type.

    :param spark: SparkSession object
    :param input_bucket: Name of the input S3 bucket
    :param output_bucket: Name of the output S3 bucket
    :param error_bucket: Name of the error S3 bucket
    :param env_name: Environment name (e.g., dev, prod)
    :param file_name: Name of the file to process
    """
    input_path = f"s3://{input_bucket}/{env_name}/staging_{file_name}"
    output_path = f"s3://{output_bucket}/{env_name}/transformation_{file_name}"

    logger.info(f"Processing file: {file_name}")
    logger.info(f"Reading data from: {input_path}")

    try:
        # Record transformation start time
        transformation_start_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Read Parquet file from S3
        df = spark.read.parquet(input_path)

        # Add transformation start time to the DataFrame
        df = df.withColumn("transformation_start_time", lit(transformation_start_time))

        if file_name == "customer_data":
            # Copy customer_data as is
            logger.info("Copying customer_data without transformation.")
        elif file_name == "transaction_data":
            # Rename column 'amounttt' to 'amount' for transaction_data
            logger.info("Renaming column 'amounttt' to 'amount' for transaction_data.")
            df = df.withColumnRenamed("amounttt", "amount")
        else:
            logger.warning(f"Unknown file type: {file_name}. Skipping transformation.")

        # Record transformation finish time
        transformation_finish_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Add transformation finish time to the DataFrame
        df = df.withColumn("transformation_finish_time", lit(transformation_finish_time))

        # Remove ingestion-related columns if they exist
        columns_to_remove = ["ingestion_start_time", "ingestion_finish_time"]
        for column in columns_to_remove:
            if column in df.columns:
                df = df.drop(column)

        # Write the processed data to S3 in Parquet format
        logger.info(f"Writing processed data to: {output_path}")
        df.write.parquet(output_path, mode="overwrite")

        logger.info(f"Successfully processed file: {file_name}")
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        error_path = f"s3://{error_bucket}/{env_name}/{file_name}_error.log"
        logger.info(f"Writing error log to: {error_path}")
        # Optionally, write error details to the error bucket


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
        ],
    )

    # Extract arguments
    env_name = args["env_name"]
    input_bucket = args["input_bucket"]
    output_bucket = args["output_bucket"]
    error_bucket = args["error_bucket"]
    file_names = args["file_names"].split(",")  # Comma-separated list of file names

    # Initialize Spark session
    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

    # Loop through files and process them
    for file_name in file_names:
        process_file(spark, input_bucket, output_bucket, error_bucket, env_name, file_name)

    # Stop the Spark session
    spark.stop()
    logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
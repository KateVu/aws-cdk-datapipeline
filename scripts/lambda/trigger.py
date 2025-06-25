import boto3
import os
from datetime import datetime

def handler(event, context):
    step_function_arn = os.environ["STEP_FUNCTION_ARN"]
    region = os.environ["REGION"]
    bucket_name = os.environ["BUCKET_NAME"]
    file_names = os.environ["FILE_NAMES"].split(",")  # Convert comma-separated string to list

    s3_client = boto3.client("s3", region_name=region)
    step_function_client = boto3.client("stepfunctions", region_name=region)

    # Extract file details from the event
    for record in event["Records"]:
        file_name = record["s3"]["object"]["key"]
        folder_path = "/".join(file_name.split("/")[:-1])  # Extract folder path
        print(f"New file detected: {file_name} in folder: {folder_path}")

        # Check if all files in file_names exist in the same folder
        all_files_exist = True
        for required_file in file_names:
            file_path = f"{folder_path}/{required_file}"  # Construct full path for each required file
            try:
                s3_client.head_object(Bucket=bucket_name, Key=file_path)
                print(f"File {file_path} exists in bucket {bucket_name}.")
            except s3_client.exceptions.ClientError:
                print(f"File {file_path} does not exist in bucket {bucket_name}.")
                all_files_exist = False
                break

        # Trigger the Step Function only if all files exist
        if all_files_exist:
            current_time = datetime.utcnow().strftime("%Y%m%d-%H%M%S")  # Get current date and time
            execution_name = f"Execution-{folder_path.replace('/', '-')}-{file_name.replace('/', '-')}-{current_time}"
            print(f"All required files exist in folder {folder_path}. Triggering Step Function with execution name: {execution_name}...")
            step_function_client.start_execution(
                stateMachineArn=step_function_arn,
                name=execution_name,
                input="{\"file_path\": \"" + folder_path + "\"}",
            )
        else:
            print(f"Not all required files exist in folder {folder_path}. Step Function will not be triggered.")
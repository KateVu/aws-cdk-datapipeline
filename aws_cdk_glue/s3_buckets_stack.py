from aws_cdk import Stack, RemovalPolicy
from aws_cdk import aws_s3 as s3
from constructs import Construct


class S3BucketsStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, bucket_names: list, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 buckets from the provided list of bucket names
        for bucket_name in bucket_names:
            s3.Bucket(
                self,
                f"S3Bucket-{bucket_name}",
                bucket_name=bucket_name,
                versioned=True,  # Enable versioning for the bucket
                removal_policy=RemovalPolicy.DESTROY,  # Retain bucket on stack deletion
                auto_delete_objects=False,  # Prevent accidental deletion of objects
            )

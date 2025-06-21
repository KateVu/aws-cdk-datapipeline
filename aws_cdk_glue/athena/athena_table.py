import os
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from constructs import Construct
import aws_cdk.aws_lakeformation as lakeformation
from aws_cdk_glue.utils.utils import add_output  # Import the add_output function


def create_glue_role(
    scope: Construct,
    id: str,
    env_name: str,
    output_bucket: str,
    account_id: str,
    region: str,
) -> iam.Role:
    """Create an IAM role for the Glue crawler with necessary permissions."""
    crawler_role = iam.Role(
        scope,
        id,
        assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        ],
    )

    # Add permissions to access the staging bucket
    crawler_role.add_to_policy(
        iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:DeleteObject",
            ],
            resources=[
                f"arn:aws:s3:::{output_bucket}",
                f"arn:aws:s3:::{output_bucket}/{env_name}/*",
            ],
        )
    )

    # Add permissions to access Athena databases
    crawler_role.add_to_policy(
        iam.PolicyStatement(
            actions=[
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:UpdateTable",
                "glue:CreateTable",
                "glue:UpdatePartition",
                "glue:GetPartition",
                "glue:BatchGetPartition",
                "glue:BatchCreatePartition",
            ],
            resources=[
                f"arn:aws:glue:{region}:{account_id}:catalog",
                f"arn:aws:glue:{region}:{account_id}:database/{env_name}_database",
            ],
        )
    )

    return crawler_role


def create_glue_table(
    scope: Construct,
    id: str,
    table_name: str,  # Pass table name as a parameter
    env_name: str,
    output_bucket: str,
    account_id: str,
    glue_database: glue.CfnDatabase,
) -> glue.CfnTable:
    """Create a Glue table for the Athena database."""
    glue_table = glue.CfnTable(
        scope,
        id,
        catalog_id=account_id,  # Assign account_id to catalog_id
        database_name=glue_database.ref,  # Reference the Glue database
        table_input={
            "name": table_name,  # Use the passed table name
            "storageDescriptor": {
                "location": f"s3://{output_bucket}/{env_name}/{table_name}/",  # Path to the data in S3
                "inputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",  # Input format for the table
                "outputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",  # Output format for the table
                "serdeInfo": {
                    "serializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",  # SerDe library
                    "parameters": {'classification': 'Parquet'},
                },
            },
            "tableType": "EXTERNAL_TABLE",  # Define the table type as external
        },
    )
    return glue_table


class AthenaTable(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_name: str,
        staging_bucket: str,
        staging_file_names: list,
        transformation_bucket: str,
        transformation_file_names: list,
        account_id: str,
        region: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)
        database_name = f"{env_name}_database"

        tag_key = "kate"
        tag_values = ["test"]

        # Define the Glue database
        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=account_id,
            database_input={"name": database_name},
            database_name=database_name,
        )

        lf_tag_pair_property = lakeformation.CfnTagAssociation.LFTagPairProperty(
            catalog_id=account_id, tag_key=tag_key, tag_values=tag_values
        )

        tag_association = lakeformation.CfnTagAssociation(
            self,
            "TagAssociation",
            lf_tags=[lf_tag_pair_property],
            resource=lakeformation.CfnTagAssociation.ResourceProperty(
                database=lakeformation.CfnTagAssociation.DatabaseResourceProperty(
                    catalog_id=account_id, name=database_name
                )
            ),
        )
        tag_association.node.add_dependency(glue_database)

        crawler_role_staging: iam.Role = create_glue_role(
            self,
            f"GlueCrawlerRoleStaging-{env_name}",
            env_name,
            staging_bucket,
            account_id,
            region,
        )

        # Grant permissions for database
        grant_staging_crawler_database_access = lakeformation.CfnPermissions(
            self,
            "LFDatabasePermissions",
            data_lake_principal={
                "dataLakePrincipalIdentifier": crawler_role_staging.role_arn
            },
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=account_id, name=database_name
                ),
            ),
            permissions=["ALTER", "DROP", "DESCRIBE", "CREATE_TABLE"],
        )

        grant_staging_crawler_database_access.node.add_dependency(glue_database, tag_association)

        for file_name in staging_file_names:
            # Remove file type from file name
            table_name = f"staging_{os.path.splitext(file_name)[0]}"  # Extract the base name without file extension

            # Create a Glue table for each file name
            glue_table = create_glue_table(
                self,
                f"StagingGlueTable-{table_name}",
                table_name=table_name,  # Use the file name as the table name
                env_name=env_name,
                output_bucket=staging_bucket,
                account_id=account_id,
                glue_database=glue_database,
            )
            # Grant permissions for tables
            grant_table_access = lakeformation.CfnPermissions(
                self,
                f"StagingGlueTableLFTablePermissions-{file_name}",
                data_lake_principal={
                    "dataLakePrincipalIdentifier": crawler_role_staging.role_arn
                },
                resource=lakeformation.CfnPermissions.ResourceProperty(
                    table_resource=lakeformation.CfnPermissions.TableResourceProperty(
                        database_name=database_name, name=table_name
                    )
                ),
                permissions=["SELECT", "ALTER", "DROP", "INSERT", "DESCRIBE"],
            )
            grant_table_access.node.add_dependency(glue_database, tag_association)

            # Output the Glue table name using add_output
            add_output(
                self,
                f"StagingGlueTableName-{table_name}",
                glue_table.ref,
            )

        # Define the Glue crawler
        glue_crawler_staging = glue.CfnCrawler(
            self,
            "GlueCrawlerStaging",
            name=f"{env_name}_staging_crawler",
            role=crawler_role_staging.role_arn,  # Use the created IAM role
            database_name=glue_database.ref,
            targets={"s3Targets": [{"path": f"s3://{staging_bucket}/{env_name}/"}]},
        )

        # Output the Glue crawler name using add_output
        add_output(
            self,
            "GlueStagingCrawlerName",
            glue_crawler_staging.ref,
        )

        # Transformation Process
        crawler_role_transformation: iam.Role = create_glue_role(
            self,
            f"GlueCrawlerRoleTransformation-{env_name}",
            env_name,
            transformation_bucket,
            account_id,
            region,
        )

        grant_transformation_crawler_database_access = lakeformation.CfnPermissions(
            self,
            "LFDatabasePermissionsTransformation",
            data_lake_principal={
                "dataLakePrincipalIdentifier": crawler_role_transformation.role_arn
            },
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=account_id, name=database_name
                ),
            ),
            permissions=["ALTER", "DROP", "DESCRIBE", "CREATE_TABLE"],
        )

        grant_transformation_crawler_database_access.node.add_dependency(glue_database, tag_association)

        for file_name in transformation_file_names:
            table_name = f"transformation_{os.path.splitext(file_name)[0]}"

            glue_table = create_glue_table(
                self,
                f"TransformationGlueTable-{table_name}",
                table_name=table_name,
                env_name=env_name,
                output_bucket=transformation_bucket,
                account_id=account_id,
                glue_database=glue_database,
            )

            grant_table_access = lakeformation.CfnPermissions(
                self,
                f"TransformationGlueTableLFTablePermissions-{file_name}",
                data_lake_principal={
                    "dataLakePrincipalIdentifier": crawler_role_transformation.role_arn
                },
                resource=lakeformation.CfnPermissions.ResourceProperty(
                    table_resource=lakeformation.CfnPermissions.TableResourceProperty(
                        database_name=database_name, name=table_name
                    )
                ),
                permissions=["SELECT", "ALTER", "DROP", "INSERT", "DESCRIBE"],
            )
            grant_table_access.node.add_dependency(glue_database, tag_association)

            # Output the Glue table name using add_output
            add_output(
                self,
                f"TransformationGlueTableName-{table_name}",
                glue_table.ref,
            )

        glue_crawler_transformation = glue.CfnCrawler(
            self,
            "GlueCrawlerTransformation",
            name=f"{env_name}_transformation_crawler",
            role=crawler_role_transformation.role_arn,
            database_name=glue_database.ref,
            targets={"s3Targets": [{"path": f"s3://{transformation_bucket}/{env_name}/"}]},
        )

        # Output the Glue crawler name using add_output
        add_output(
            self,
            "GlueTransformationCrawlerName",
            glue_crawler_transformation.ref,
        )

        self.glue_crawler_staging = glue_crawler_staging
        self.glue_crawler_transformation = glue_crawler_transformation

from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import CfnOutput
from constructs import Construct
import aws_cdk.aws_lakeformation as lakeformation


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
                f"arn:aws:s3:::{output_bucket}/{env_name}*",
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
                "location": f"s3://{output_bucket}/{env_name}/",  # Path to the data in S3
                "inputFormat": "org.apache.hadoop.mapred.TextInputFormat",  # Input format for the table
                "outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # Output format for the table
                "serdeInfo": {
                    "serializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",  # SerDe library
                    "parameters": {"field.delim": ","},  # Field delimiter for CSV files
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
        account_id: str,
        region: str,
        staging_file_names: list,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)
        crawler_role_staging: iam.Role = create_glue_role(
            self,
            f"GlueCrawlerRoleStaging-{env_name}",
            env_name,
            staging_bucket,
            account_id,
            region,
        )
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
                # catalog=account_id,
                database=lakeformation.CfnTagAssociation.DatabaseResourceProperty(
                    catalog_id=account_id, name=database_name
                )
            ),
        )

        # Grant permissions for database
        grant_database_access = lakeformation.CfnPermissions(
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

        tag_association.node.add_dependency(glue_database)
        grant_database_access.node.add_dependency(glue_database, tag_association)

        for file_name in staging_file_names:
            # Create a Glue table for each file name
            glue_table = create_glue_table(
                self,
                f"StagingGlueTable-{file_name}",
                table_name=file_name,  # Use the file name as the table name
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
                        database_name=database_name, name=file_name
                    )
                ),
                permissions=["SELECT", "ALTER", "DROP", "INSERT", "DESCRIBE"],
            )
            grant_table_access.node.add_dependency(glue_database, tag_association)

            # Output the Glue table name
            CfnOutput(
                self,
                f"StagingGlueTableName-{file_name}",
                value=glue_table.ref,
                description=f"The name of the Staging Glue table for {file_name}",
            )

        # Define the Glue crawler
        glue_crawler_staging = glue.CfnCrawler(
            self,
            "GlueCrawlerStaging",
            name=f"{env_name}_staging_crawler",
            role=crawler_role_staging.role_arn,  # Use the created IAM role
            database_name=glue_database.ref,
            targets={"s3Targets": [{"path": f"s3://{staging_bucket}/{env_name}"}]},
        )

        # Output the Glue crawler name
        CfnOutput(
            self,
            "GlueCrawlerName",
            value=glue_crawler_staging.ref,
            description="The name of the Glue crawler for the staging bucket",
        )

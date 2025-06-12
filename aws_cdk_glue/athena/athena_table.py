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
    crawler_role: iam.Role = iam.Role(
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


class AthenaTable(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        env_name: str,
        output_bucket: str,
        account_id: str,
        region: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)
        crawler_role_staging: iam.Role = create_glue_role(
            self,
            f"GlueCrawlerRoleStaging-{env_name}",
            env_name,
            output_bucket,
            account_id,
            region,
        )

        tag_key = "kate"
        tag_values = ["test"]

        # Define the Glue database
        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=account_id,  # Assign account_id to catalog_id
            database_input={"name": f"{env_name}_database"},
            database_name=f"{env_name}_database",
        )

        # Define the Glue table
        # glue_table = glue.CfnTable(
        #     self,
        #     "GlueTable",
        #     catalog_id=account_id,  # Assign account_id to catalog_id
        #     database_name=glue_database.ref,
        #     table_input={
        #         "name": f"{env_name}_staging_table",
        #         "storageDescriptor": {
        #             "location": f"s3://{output_bucket}/{env_name}/test",
        #             "inputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        #             "outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        #             "serdeInfo": {
        #                 "serializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        #                 "parameters": {"field.delim": ","},
        #             },
        #         },
        #         "tableType": "EXTERNAL_TABLE",
        #     },
        # )

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
                    catalog_id=account_id, name=f"{env_name}_database"
                )
            ),
        )

        # Grant permissions for database
        lakeformation.CfnPermissions(
            self,
            "LFDatabasePermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": crawler_role.role_arn},
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=account_id, name=f"{env_name}_database"
                ),
            ),
            permissions=["ALTER", "DROP", "DESCRIBE", "CREATE_TABLE"],
        )

        # Grant permissions for tables
        lakeformation.CfnPermissions(
            self,
            "LFTagPermissions",
            data_lake_principal={"dataLakePrincipalIdentifier": crawler_role.role_arn},
            resource=lakeformation.CfnPermissions.ResourceProperty(
                table_resource=lakeformation.CfnPermissions.TableResourceProperty(
                    database_name=f"{env_name}_database",
                    table_wildcard=lakeformation.CfnPermissions.TableWildcardProperty(),
                )
            ),
            permissions=["SELECT", "ALTER", "DROP", "INSERT", "DESCRIBE"],
        )

        # tag_association.node.add_dependency(cfn_tag)
        tag_association.node.add_dependency(glue_database)

        # Define the Glue crawler
        glue_crawler = glue.CfnCrawler(
            self,
            "GlueCrawler",
            name=f"{env_name}_staging_crawler",
            role=crawler_role_staging.role_arn,  # Use the created IAM role
            database_name=glue_database.ref,
            targets={"s3Targets": [{"path": f"s3://{output_bucket}/{env_name}"}]},
        )

        # # Output the Athena table name
        # CfnOutput(
        #     self,
        #     "AthenaTableName",
        #     value=glue_table.ref,
        #     description="The name of the Athena table for the staging bucket",
        # )

        # Output the Glue crawler name
        CfnOutput(
            self,
            "GlueCrawlerName",
            value=glue_crawler.ref,
            description="The name of the Glue crawler for the staging bucket",
        )

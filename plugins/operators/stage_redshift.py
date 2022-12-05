"""
Operator to load any JSON formatted files from S3 to Amazon Redshift.

Functionalities:
1. Create and run a SQL COPY statement based on the parameters provided.
2. Ability to distinguish between JSON files.
3. Allows to load timestamped files from S3 based on the execution time
    and run backfills.
"""

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Airflow operator to load JSON formatted files from S3 to Redshift.

    :param redshift_conn_id: (Optional) Amazon Redshift connection id
    :param aws_credentials_id: (Optional) AWS credentials id
    :param table: (Optional) Table name (format: {schema}.{table})
    :param s3_bucket: (Optional) S3 bucket name
    :param s3_key: (Optional) S3 bucket key
    :param json_path: (Optional) json path for format of copy
    """

    ui_color = '#89DA59'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 table: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 json_path: str = "auto",
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context) -> None:
        """Copy data from s3 to Amazon Redshift."""
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # delete data if exists
        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")
        # render s3 path based on inputs
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        # format copy statement from s3 to Amazon Redshift
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift.run(formatted_sql)

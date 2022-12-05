"""Airflow operator to load a fact table to Redshift."""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Executes SQL query and add to table on an Amazon Redshift cluster.

    :param redshift_conn_id: (Optional) Amazon Redshift connection id
    :param table: (Optional) Fact table name (format: {schema}.{table})
    :param query: (Optional) Query to execute to load data to fact table
    :param append: (Optional) if True append data, if False delete and insert
        (default value: False)
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 table: str = "",
                 query: str = "",
                 append: bool = False,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append = append

    def execute(self, context) -> None:
        """Create and run a SQL to load data in a fact table."""
        redshift = PostgresHook(self.redshift_conn_id)
        if not self.append:
            # delete data to table
            self.log.info(f"Delete data to {self.table} in Redshift")
            delete_sql = f"DELETE FROM {self.table}"
            redshift.run(delete_sql)

        # insert data to table
        self.log.info(f"Insert data to {self.table} in Redshift")
        insert_sql = f"INSERT INTO {self.table}\n" + self.query
        redshift.run(insert_sql)

"""
Data Quality Operator.

The operator's main functionality is to receive one or more SQL
based test cases along with the expected results and execute the tests.
For each, the test result and expected result will to be checked and if
there is no match, the operator will raise an exception and the task will
retry and fail eventually.
"""

from typing import Dict

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Executes SQL queries for data quality checks.

    :param redshift_conn_id: (Optional) Amazon Redshift connection id
    :param query_check_dict: (Optional) Dict containing key-value pairs of
        data check queries and expected results
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 query_check_dict: Dict = None,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_check_dict = query_check_dict

    def execute(self, context) -> None:
        """Run data quality check."""
        redshift = PostgresHook(self.redshift_conn_id)
        if self.query_check_dict is not None:
            for query, result in self.query_check_dict.items():
                records = redshift.get_records(query)
                num_records = records[0][0]
                if num_records != result:
                    raise ValueError(f"Data quality check failed! "
                                     f"{query} returned {num_records}, "
                                     f"expected {result}")
                self.log.info(f"Data quality check {query} passed with "
                              f"{num_records} records")

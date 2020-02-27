from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import array
"""
    DataQualityOperator checks to see if a specified column in the table has NULL value and raises an error if a NULL row is found.
    redshift connection
    list of table names
    list of column names 
    ** This operator assumes that the one column name per table and that columns are listed in the order of table to which the column belongs
"""
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 tables=[],
                 columns=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables
        self.columns=columns

    def execute(self, context):
        self.log.info('Checking on DataQuality')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i in range(len(self.tables)):
            sql_stmt = "SELECT COUNT(*) FROM {} where {} IS NULL".format(self.tables[i], self.columns[i])
            records = redshift_hook.get_records(sql_stmt)
            if len(records) > 1 or len(records[0]) > 1:
                raise ValueError(f"Data quality check failed. {self.tables[i]} returned results with NULL rows")
            num_records = records[0][0]
            if num_records > 1:
                raise ValueError(f"Data quality check failed. {self.tables[i]} contained {num_records} NULL rows")
            self.log.info(f"Data quality on table {self.tables[i]} check passed with 0 NULL records")

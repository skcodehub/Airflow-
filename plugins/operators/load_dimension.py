from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
LoadDimensionOperator loads dimension tables to Redshift based on the query provided with an option to truncate dimension table each time
    redshift connection
    boolean indicator to truncate dimension
    dimension table name
    insert statement for fact table
"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table = "",
                 del_ind = False,
                 dim_insert="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.dim_insert = dim_insert
        self.del_ind = del_ind
        self.dim_insert = dim_insert

    def execute(self, context):
        self.log.info('LoadDimensionOperator executing to load dimensions')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        del_stmt = "TRUNCATE {}".format(self.table)
        if self.del_ind:
            redshift.run(del_stmt)
        dim_insert = self.dim_insert
        redshift.run(dim_insert)

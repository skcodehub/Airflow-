from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
    LoadFactOperator loads dimension tables to Redshift based on the query provided with an option to truncate fact table each time
    redshift connection
    boolean indicator to delete fact
    fact table name
    insert statement for fact table    
"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table = "",
                 del_ind = False,
                 fact_insert="",
                 *args, **kwargs):
                 
                
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.fact_insert = fact_insert
        self.del_ind = del_ind
        self.fact_insert = fact_insert
        
    def execute(self, context):
        self.log.info('Loading fact table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        st_date = context.get("ds")
        del_stmt = "DELETE FROM {} WHERE TRUNC(start_time) ='{}'".format(self.table, st_date)  
        if self.del_ind:
            redshift.run(del_stmt)
        fact_insert = self.fact_insert
        redshift.run(fact_insert)

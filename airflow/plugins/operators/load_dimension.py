from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_insert_query="",
                 truncate_table=True,
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.sql_insert_query = sql_insert_query,
        self.table = table,
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('LoadDimensionOperator has started')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table}')
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info(f'Running query {self.query}')
        redshift.run(f"Insert into {self.table} {self.query}")
        
        self.log.info(f'Success: Loading the dimension table')
        
        

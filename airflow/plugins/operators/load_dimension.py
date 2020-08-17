from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_insert_query="",
                 truncate_table="",
                 table="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.sql_insert_query = sql_insert_query,
        self.table = table,
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('LoadDimensionOperator has started')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            sql_statement = "INSERT INTO {self.table} {self.query}"
            redshift.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table_name
            redshift.run(sql_statement)

            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
            redshift.run(sql_statement)
        

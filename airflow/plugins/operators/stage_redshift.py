from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    staging_events_copy = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}';
     """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 file_type="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        # Connections
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id =self.redshift_conn_id)
        
        # Clear existing raw data
        self.log.info('Clearing data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))
        
        # copy the new raw data
        self.log.info(f"In progress: Copying {self.table} from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table_name = self.table,
            s3_path = s3_path,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            file_type = self.file_type
        )
        redshift.run(formatted_sql)
        
        self.log.info(f'Done: Copying {self.table} from S3 to Redshift')


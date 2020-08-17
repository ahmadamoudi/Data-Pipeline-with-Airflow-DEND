from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ahmad',
    'start_date': datetime(2020, 8, 14),
    'email': ['ahmad.****@outlook.com'],
    'depends_on_past': False ,
    'retries': 3 ,
    'retry_delay': timedelta(minutes=5) ,
    'catchup': False

}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          #,catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="[public].staging_events",
    redshift_conn_id="redshift",
    aws_credintials_id="aws_default",
    s3_bucket="udacity-dend",
    s3_key="log_data" ,
    file_type="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="[public].staging_songs",
    redshift_conn_id="redshift",
    aws_credintials_id="aws_default",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A" ,
    file_type="JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="[public].songplays",
    redshift_conn_id="redshift",
    sql_insert_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="[public].users",
    redshift_conn_id="redshift",
    sql_insert_query=SqlQueries.user_table_insert,
    append_data=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="[public].songs",
    redshift_conn_id="redshift",
    sql_insert_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="[public].artists",
    redshift_conn_id="redshift",
    sql_insert_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="[public].time",
    redshift_conn_id="redshift",
    sql_insert_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table=["songplays", "users", "songs", "artists", "time"],
    test_query=[
{'check_sql': "SELECT COUNT() FROM users WHERE userid is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT() FROM songs WHERE song_id is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT() FROM songplays WHERE songplay_id is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT() FROM artists WHERE artist_id is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT() FROM time WHERE start_time is null", 'expected_result': 0}
],
    expected_result=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift,
                   stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table,
                                                                        load_song_dimension_table, 
                                                                        load_artist_dimension_table, 
                                                                        load_time_dimension_table] >> run_quality_checks >> end_operator

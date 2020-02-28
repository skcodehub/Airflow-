from datetime import datetime, timedelta
import array
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
"""
  This dag contains calls to other Operators to load data from S3 to Redshift stage tables, and then load from 
  stage to fact and dimension tables. Once the tables are loaded, there is call to a data quality check operator that checks for NULL values.
  The runtime properties among others are 
  1. to catch up past runs, job starts on 2018-11-1 and ends 2018-11-30
  2. future runs wait for past runs to complete, 
  3. retries 3 times upon failure and 
  4. runs within 5 minutes of failure
  
"""
default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'email': 'airflow@example.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          max_active_runs=1,
          catchup=True,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          start_date = datetime(2018, 11, 1,0,0,0,0),
          end_date = datetime(2018, 11, 5,0,0,0,0)
          #schedule_interval='@daily'
          #schedule_interval="@once"
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table = "staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    provide_context=True,
    #s3_key="log_data",
    #st_date=default_args['start_date'],
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table = "staging_songs",
    s3_bucket="udacity-dend",
    #s3_key="song_data/A/B/C/TRABCEI128F424C983.json",
    s3_key="song_data/",
    #st_date=default_args['start_date'],
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    table = "public.songplays",
    del_ind = True,
    fact_insert = "INSERT INTO public.songplays {}".format(SqlQueries.songplay_table_insert)
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.users",
    del_ind = True,
    dim_insert = "INSERT INTO public.users {}".format(SqlQueries.user_table_insert)
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.songs",
    del_ind = True,
    dim_insert = "INSERT INTO public.songs {}".format(SqlQueries.song_table_insert)
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.artists",
    del_ind = True,
    dim_insert = "INSERT INTO public.artists {}".format(SqlQueries.artist_table_insert)
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.time_table",
    del_ind = True,
    dim_insert = "INSERT INTO public.time_table {}".format(SqlQueries.time_table_insert)
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["users", "artists", "songs", "time_table", "songplays"],
    columns=["userid", "artistid", "songid", "start_time", "userid"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks  >> end_operator

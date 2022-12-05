"""Sparkfy Dag to populate fact and dimension tables and quality check them."""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (LoadFactOperator, LoadDimensionOperator,
                               DataQualityOperator, StageToRedshiftOperator)
from helpers import SqlQueries


# Get AWS configs from Airflow environment
AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'danai',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 4, 0, 0, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('sparkify',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'  # once an hour
          )


start_operator = DummyOperator(task_id='begin_execution',
                               dag=dag)

create_tables_to_redshift = PostgresOperator(
    task_id='create_tables_to_redshift',
    dag=dag,
    sql='resources/create_tables.sql',
    postgres_conn_id="redshift",
    autocommit=True
    )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table='public.staging_events',
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data/",
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table='public.staging_songs',
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song-data/A/A/A/"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    query=SqlQueries.songplay_table_insert,
    append=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.users",
    query=SqlQueries.user_table_insert,
    append=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songs",
    query=SqlQueries.song_table_insert,
    append=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.artists",
    query=SqlQueries.artist_table_insert,
    append=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.time",
    query=SqlQueries.time_table_insert,
    append=False
)

run_quality_checks = DataQualityOperator(
    task_id='data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_check_dict={
        'SELECT COUNT(*) FROM public.artists WHERE artistid is Null': 0,
        'SELECT COUNT(*) FROM public.songplays WHERE playid is Null': 0,
        'SELECT COUNT(*) FROM public.songplays WHERE start_time is Null': 0,
        'SELECT COUNT(*) FROM public.songplays WHERE userid is Null': 0,
        'SELECT COUNT(*) FROM public.songs WHERE songid is Null': 0,
        'SELECT COUNT(*) FROM public."time" WHERE start_time is Null': 0,
        'SELECT COUNT(*) FROM public.users WHERE userid is Null': 0
    }
)

end_operator = DummyOperator(task_id='stop_execution',
                             dag=dag)


start_operator >> create_tables_to_redshift

create_tables_to_redshift >> [stage_events_to_redshift,
                              stage_songs_to_redshift]

[stage_events_to_redshift,
 stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table,
                         load_song_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table]

[load_user_dimension_table,
 load_song_dimension_table,
 load_artist_dimension_table,
 load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator

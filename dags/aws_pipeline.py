from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (RedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

debug = False

default_args = {
    'owner': 'KOSEMANI',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('DataPipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
          )

start = DummyOperator(task_id='start',  dag=dag)

events_to_redshift = RedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    sqls=SqlQueries.event_sqls,
    debug=debug,
    delete=SqlQueries.drop_events_table,
    dag=dag
)

songs_to_redshift = RedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    sqls=SqlQueries.songs_sqls,
    debug=debug,
    delete=SqlQueries.drop_songs_table,
    dag=dag
)

songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    sqls=SqlQueries.facttable_sqls,
    debug=debug,
    delete=SqlQueries.drop_songplay_table,
    dag=dag
)

user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    sqls=SqlQueries.user_dim_sqls,
    debug=debug,
    delete=SqlQueries.drop_user_table,
    dag=dag
)

song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    sqls=SqlQueries.song_dim_sqls,
    debug=debug,
    delete=SqlQueries.drop_song_table,
    dag=dag
)

artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    sqls=SqlQueries.artist_dim_sqls,
    debug=debug,
    delete=SqlQueries.drop_artist_table,
    dag=dag
)

time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    sqls=SqlQueries.time_dim_sqls,
    debug=debug,
    delete=SqlQueries.drop_time_table,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Quality_check',
    redshift_conn_id='redshift',
    sparkify_tables=SqlQueries.tables,
    sqls=SqlQueries.counter,
    debug=debug,
    dag=dag
)

end = DummyOperator(task_id='End',  dag=dag)

start >> events_to_redshift
start >> songs_to_redshift
events_to_redshift >> songplays_table
songs_to_redshift >> songplays_table
songplays_table >> song_dimension_table
songplays_table >> user_dimension_table
songplays_table >> artist_dimension_table
songplays_table >> time_dimension_table
song_dimension_table >> run_quality_checks
user_dimension_table >> run_quality_checks
artist_dimension_table >> run_quality_checks
time_dimension_table >> run_quality_checks
run_quality_checks >> end

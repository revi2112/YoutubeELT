from airflow import DAG
from dags.datawerehouse.raw_loader import load_raw_data_from_s3
from datawerehouse.dwh import core_table, staging_table
from dataquality.soda import yt_elt_data_quality
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_s3,
)

channel_handles = Variable.get("CHANNEL_HANDLES", deserialize_json = True)
local_tz = pendulum.timezone("America/Chicago")

#args that can reused
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 8, 4, tzinfo=local_tz), # time at which airflow will begin running the dag, first run will bes scheduled at end of interraval, following next day
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

# extract
with DAG(
    dag_id="produce_s3_snapshot",
    default_args = default_args,
    description="DAG to pull YouTube stats for each configured channel and land raw snapshots in S3",
    schedule = "0 14 * * *", #cron tab guru
    catchup = False # not to catch up missed 
) as dag_update:
    
    #expand() for dynamic taks mapping, runs task for each instance in parlled and isolated and independed 
    # in bg the map_index(len of list) will be created as a pointer Xcom(playlist id)
    #instance with map_index=0 receives exactly the output from the get_playlist_id instance with map_index=0
    playlist_id = get_playlist_id.expand(channel_handle=channel_handles)
    video_ids = get_video_ids.expand(playlist_id)
    extract_data = extract_video_data.expand(video_ids)
    s3_keys = save_to_s3.expand(extract_data)
    #s3_key is also xcom pointer with mapped output of 3 instances
    #file path for obj inside bucket #reads this to load raw table
    
    #defining dependencies in dag
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
        conf = { #
            "s3_keys": "{{ ti.xcom_pull(task_ids='save_to_s3') }}",
            "snapshot_date": "{{ ds }}",
        }
    )
    s3_keys >> trigger_update_db
    
#load     

# 1. reads the S3 snapshot(s) written by produce_s3_snapshot and inserts them as new, append-only rows into raw.youtube_video_snapshots.
# Replaces the old upsert/delete staging + core logic entirely - history now on.
with DAG(
    dag_id = "update_db",
    default_args = default_args,
    description = "DAG to process JSON file and insert data into both staging and core schemas",
    schedule = None, #cron tab guru
    catchup = False # not to catch up missed 
) as dag_produce:
    
    load_raw = load_raw_data_from_s3()
 
    #defining dependencies in dag
    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    load_raw >> trigger_data_quality


#data quality 
#check raw for on then dbt
with DAG(
    dag_id = "data_quality",
    default_args = default_args,
    description = "DAG to check data quality on raw schema",
    schedule = None, #cron tab guru
    catchup = False # not to catch up missed 
) as dag_quality:
    
      # Define tasks
    soda_validate_raw = yt_elt_data_quality("raw")

    # Define dependencies

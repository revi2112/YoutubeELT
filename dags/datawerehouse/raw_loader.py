#get pg con and cur (get from variables)
# create schema, table 
#get s3 vars and connect to s3 using hook
# create task to load from s3
# use logging for logs

import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

logger = logging.getLogger(__name__)

RAW_SCHEMA = "raw"
RAW_TABLE = "youtube_video_snapshots"

def _get_pg_conn_cursor():
    #create the hook and get conn and from conn get cursor 
    hook = PostgresHook(postgres_conn_id = "postgres_db_yt_elt", database = "elt_db" )
    conn = hook.get_conn()
    cur = conn.cursor()
    return conn, cur

def _create_schema_table():
    conn, cur = _get_pg_conn_cursor()
    try:
        #schema 
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
        
        cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
                        video_id VARCHAR(200) NOT NULL, 
                        channel_id VARCHAR(200) NOT NULL,
                        channel_handle VARCHAR(200) NOT NULL,
                        title text,
                        published_at TIMESTAMP,
                        duration VARCHAR(30),
                        view_count BIGINT,
                        like_count BIGINT,
                        comment_count BIGINT,
                        snapshot_date DATE NOT NULL,
                        loaded_at TIMESTAMP NOT NULL DEFAULT now()
                        PRIMARY KEY (video_id, snapshot_date)
                    );
                    """)
    
    finally:
        cur.close()
        conn.close() 

#using s3 key get the data and upload 

@task
def load_raw_data_from_s3(**context):
    """
    Reads every snapshot file written by todays produce_S3_snapshot dag run (once per channel) and inserts each video as a row 
    keyed by (video_id, snapshot_date). only insert never update or del.
    has history.
    """
    
    _create_schema_table()
    
    
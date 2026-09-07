#get pg con and cur (get from variables)
# create schema, table 
#get s3 vars and connect to s3 using hook
# create task to load from s3
# use logging for logs

import json
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.models import Variable

logger = logging.getLogger(__name__)

RAW_SCHEMA = "raw"
RAW_TABLE = "youtube_video_snapshots"
AWS_CONN_ID = "aws_default"
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")

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
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed loading raw snapshots: {e}")
        raise e
    
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
    
    dag_run = context["dag_run"]
    s3_keys = dag_run.conf["s3_keys"]
    snapshot_date = dag_run.conf["snapshot_date"]
    
    #create hook to connect s3.
    s3_hook = S3Hook(aws_conn_id = AWS_CONN_ID)
    conn, cur = _get_pg_conn_cursor()
    
    inserted = 0
    
    try:
        for s3_key in s3_keys:
            raw_body = s3_hook.read_key(key = s3_key, bucket_name = S3_BUCKET_NAME)
            videos = json.loads(raw_body)
            for video in videos:
                #psycopg2 s is type cast maker reat this as a string/generic value,  % put value in this pl;aceholder 
                cur.execute( 
                    f"""
                    INSERT INTO {RAW_SCHEMA}.{RAW_TABLE}
                        (video_id, channel_id, channel_handle, title, published_at,
                         duration, view_count, like_count, comment_count, snapshot_date)
                    VALUES
                        (%(video_id)s, %(channel_id)s, %(channel_handle)s, %(title)s, %(publishedAt)s,
                         %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s, %(snapshot_date)s)
                    ON CONFLICT (video_id, snapshot_date) DO NOTHING;
                    """,
                    {**video, "snapshot_date": snapshot_date},
                )
                #how many rows did the last statement affect not all rows / count
                inserted += cur.rowcount
 
            conn.commit()
            logger.info(f"Inserted {inserted} new rows into {RAW_SCHEMA}.{RAW_TABLE}")
                
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed loading raw snapshots: {e}")
        raise e
    
    finally:
        cur.close()
        conn.close()
        
    return inserted #how many rows landed today.
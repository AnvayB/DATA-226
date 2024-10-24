from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago

from datetime import datetime
import logging


def return_snowflake_conn():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def set_stage():
    cur = return_snowflake_conn()
    
    create_stage_sql = """
    CREATE OR REPLACE STAGE stock_db.raw_data.blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """
    
    copy_user_session_sql = """
    COPY INTO stock_db.raw_data.user_session_channel
    FROM @stock_db.raw_data.blob_stage/user_session_channel.csv;
    """
    
    copy_session_timestamp_sql = """
    COPY INTO stock_db.raw_data.session_timestamp
    FROM @stock_db.raw_data.blob_stage/session_timestamp.csv;
    """
    
    logging.info("Running stage setup SQL")
    
    cur.execute(create_stage_sql)
    cur.execute(copy_user_session_sql)
    cur.execute(copy_session_timestamp_sql)

@task
def load():
    cur = return_snowflake_conn()

    try:
        # Start transaction
        cur.execute("BEGIN;")
        
        create_user_session_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_db.raw_data.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'
        );
        """
        
        logging.info("Running table creation SQL for user_session_channel")
        cur.execute(create_user_session_table_sql)
        
        create_session_timestamp_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_db.raw_data.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp  
        );
        """
        
        logging.info("Running table creation SQL for session_timestamp")
        cur.execute(create_session_timestamp_table_sql)

        cur.execute("COMMIT;")
        
    except Exception as e:
        # Rollback the transaction in case of any failure
        cur.execute("ROLLBACK;")
        logging.error('Failed to execute table creation SQL. Completed ROLLBACK!')
        logging.error(str(e))
        raise


with DAG(
    dag_id='ETL-pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_retry': False
    },
    description='ETL Pipeline to Snowflake using Airflow',
    start_date=datetime(2024, 10, 5),  
    schedule_interval='@daily', 
    catchup=False,
    tags=['snowflake', 'ETL'],
) as dag:

    stage_task = set_stage()
    load_task = load()

    stage_task >> load_task

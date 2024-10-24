from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

"""
This pipeline assumes that there are two other tables in your Snowflake DB:
 - user_session_channel
 - session_timestamp

For how to set up these two tables, refer to the README:
 - https://github.com/keeyong/sjsu-data226/blob/main/week9/How-to-setup-ETL-tables-for-ELT.md
"""

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(table, select_sql, primary_key=None):
    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # We check for primary key uniqueness here
        if primary_key is not None:
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
            logging.info(sql)
            cur.execute(sql)
            result = cur.fetchone()
            if int(result[1]) > 1:
                raise Exception(f"Primary key uniqueness failed: {result}")

        # We check for duplicate records based on sessionId and timestamp
        duplicate_timestamp_check_sql = """
        SELECT sessionId, ts, COUNT(1) AS cnt
        FROM {table}
        GROUP BY sessionId, ts
        HAVING COUNT(1) > 1
        LIMIT 1
        """
        logging.info("Running additional duplicate check based on sessionId and timestamp.")
        cur.execute(duplicate_timestamp_check_sql.format(table=table))
        result = cur.fetchone()
        if result:
            raise Exception(f"Duplicate records found for sessionId: {result[0]}, timestamp: {result[1]}")

        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to execute SQL. Completed ROLLBACK!')
        raise

with DAG(
    dag_id='ELT-pipeline',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ELT'],
    schedule='45 2 * * *'
) as dag:

    table = "stock_db.analytics.session_summary"
    select_sql = """
    SELECT u.*, s.ts
    FROM stock_db.raw_data.user_session_channel u
    JOIN stock_db.raw_data.session_timestamp s ON u.sessionId = s.sessionId
    """

    ctas_task = run_ctas(table, select_sql, primary_key='sessionId')

ctas_task

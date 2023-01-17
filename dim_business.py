import os
import json
import time
import pendulum
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.models import XCom
from airflow.models import Variable
from google.oauth2 import service_account
from airflow.hooks.base_hook import BaseHook
from airflow.utils.db import provide_session
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

doc_info = """
### dim_business

### PURPOSE
Create table for dim_business

### NOTES

"""

def get_data_load():
    src = PostgresHook(postgres_conn_id='POSTGRES')
    dest = PostgresHook(postgres_conn_id='POSTGRES')
    dest_engine = dest.get_sqlalchemy_engine()
    src_conn = src.get_conn()
    scr_cursor = src_conn.cursor()

    fd = open('/opt/airflow/dags/utils/sql/get_data_dim_business.sql', 'r')
    sqlFile = fd.read()
    scr_cursor.execute(sqlFile)
    rows = scr_cursor.fetchall()
    fd.close()
    col_names = []
    for names in scr_cursor.description:
        col_names.append(names[0])
    new = pd.DataFrame(rows, columns=col_names)
    new["data_ingestion_time"] = pd.to_datetime(pendulum.now(tz = "Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S"))

    new.to_sql(
        'dim_business',
        dest_engine, 
        if_exists='append', 
        index=False, 
        schema='dwh',
        chunksize=100, 
        )
    src_conn.close()
    print('success')

@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

default_args = {
    'owner': 'Dilla',
    'retries': 1,
    'retry_delay' : timedelta(minutes=1),
}

with DAG(
        dag_id='yelp_academic_dataset_checkin',
        default_args = default_args,
        schedule_interval='5 */8 * * *',
        start_date=pendulum.datetime(2021, 1, 1, tz='Asia/Bangkok'), 
        doc_md = doc_info,
        catchup=False,
        template_searchpath = ['/opt/airflow/dags/utils/sql'],
        tags=['dwh'],
        
) as dag:
    start_task = DummyOperator(
        task_id = "start",
        trigger_rule = "all_done"
    )

    with TaskGroup("dataset_checkin") as extract_group:
        create_table = PostgresOperator(      
                task_id = "create_table_task",
                postgres_conn_id = "POSTGRES",
                sql = 'utils/sql/create_table_dim_business.sql',
                dag = dag
                )
    
        get_update_load_data = PythonOperator(
                task_id = 'get_update_load',
                python_callable = get_data_load,
                pool = 'load_data'
                )

    create_table >> get_update_load_data

    delete_xcom = PythonOperator(
        task_id = "delete_xcom",
        python_callable = cleanup_xcom,
        provide_context = True
    )

    end_task = DummyOperator(
            task_id = "finish",
            trigger_rule = "all_done"
        )

    start_task >> extract_group >> delete_xcom >> end_task
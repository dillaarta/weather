from google.cloud import storage
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
### dataset_tip

### PURPOSE
Create table for dataset_tip from yelp_academic_dataset_tip

### NOTES

"""

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/opt/airflow/dags/utils/cred.json'
cred = json.loads(Variable.get("google_cred"))
credentials = service_account.Credentials.from_service_account_info(cred)

def get_data_load():
    time.sleep(5)
    bucket_name = 'mystery_bag'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    dest = PostgresHook(postgres_conn_id="POSTGRES")
    dest_engine = dest.get_sqlalchemy_engine()
    blobs = storage_client.list_blobs(bucket_name,prefix='weather/yelp_academic_dataset_tip/')
    all_name = []
    for blob in blobs:
        if not blob.name.endswith('/'):
            all_name.append(blob.name)
    new = all_name
    
    for files in new:
        blob = bucket.get_blob(files)

        for i in json.loads(blob.download_as_string()):
            df = pd.json_normalize(i,max_level=3)
            df = df.applymap(str)
            df["data_updated_at"] = pd.to_datetime(pendulum.now(tz = "Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S"))

            df.to_sql(
                'dataset_tip',
                dest_engine, 
                if_exists='append', 
                index=False, 
                schema='ods',
                chunksize=100)
        print('success')
    time.sleep(7)

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
        dag_id='yelp_academic_dataset_tip',
        default_args = default_args,
        schedule_interval='5 */8 * * *',
        start_date=pendulum.datetime(2021, 1, 1, tz='Asia/Bangkok'), 
        doc_md = doc_info,
        catchup=False,
        template_searchpath = ['/opt/airflow/dags/utils/sql'],
        tags=['ods'],
        
) as dag:
    start_task = DummyOperator(
        task_id = "start",
        trigger_rule = "all_done"
    )

    with TaskGroup("dataset_tip") as extract_group:
        create_table = PostgresOperator(      
                task_id = "create_table_task",
                postgres_conn_id = "POSTGRES",
                sql = 'utils/sql/create_table_tip.sql',
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
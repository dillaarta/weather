import json
import time
import csv
import glob
import pendulum
from airflow import DAG
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


doc_info = """
### <DOCUMENT TITLE>

### PURPOSE
<describe main purpose of this DAG>

### NOTES
<insert link documentation of this DAG>
"""

default_args = {
    'owner': 'Dilla',
    'retries': 0,
    # 'retry_delay' : timedelta(minutes=2),
    # 'on_failure_callback': task_fail_slack_alert,
    # 'execution_timeout': timedelta(minutes=30)
}

@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

def convert_json_csv():
    path = "/opt/airflow/dags/yelp_dataset/*.json" 
    name_file = list(glob.glob(path))
    output_file = [name.rstrip('json')+'csv' for name in name_file]
    try:
        for input,output in zip(name_file, output_file):
            count = 0
            with open(input, 'r') as json_file:
                with open(output, 'w') as f:
                    for i in json_file:
                        print(i)
                        data = json.loads(i)
                        writer = csv.writer(f)
                        if count == 0:
                            header = data.keys()
                            writer.writerow(header)
                            count += 1
                        writer.writerow(data.values())
                    time.sleep(5)
    except:
        print('nothing')
with DAG(
        dag_id='convert_json_csv',
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz='Asia/Bangkok'), 
        catchup=False,
        default_args = default_args,
        # template_searchpath = ['/opt/airflow/dags/utils/sql'],
        tags=[]
) as dag:
    start_task = DummyOperator(
        task_id = "start",
        trigger_rule = "all_done"
    )

    convert_json_csv_task = PythonOperator(
        task_id = "convert_json_csv_task",
        python_callable = convert_json_csv
    )

    delete_xcom = PythonOperator(
        task_id = "delete_xcom",
        python_callable = cleanup_xcom,
        provide_context = True
    )

    end_task = DummyOperator(
            task_id = "finish",
            trigger_rule = "all_done"
        )

    start_task >> convert_json_csv_task >> delete_xcom >> end_task
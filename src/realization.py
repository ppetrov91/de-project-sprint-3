import requests
import json
import pandas as pd
import time


from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator



def _get_base_url_headers_from_xcom(ti):
    return (ti.xcom_pull(key=key, task_ids="t_get_base_url_and_headers") for key in ("headers", "base_url"))


def get_report_info(ti):
    # Get headers, base_url and task_id from xcom
    headers, base_url = _get_base_url_headers_from_xcom(ti)
    task_id = ti.xcom_pull(key="task_id", task_ids="t_create_task_for_report_generation")
    report_id, api = None, "get_report"
    
    '''
    We can request to get_report multiple times since it requires some time to generate files
    If status != 'SUCCESS' then sleep for 10 seconds and try again.
    If we fail to get 'SUCCESS' status after 20 times then raise TimeOutError exception
    '''

    for i in range(20):
        print(f"Start making {i} request to {api}")
        response = requests.get(f"{base_url}/{api}?task_id={task_id}", headers=headers)
        print(f"Stop making {i} request to {api}")
        response.raise_for_status()
        status = json.loads(response.content)["status"]
        print(f"{i} request status: {status}")

        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            s3_path = json.loads(response.content)['data']['s3_path']
            break

        time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    ti.xcom_push(key='s3_path', value=s3_path)


def create_task_for_report_generation(ti):
    # Get headers and base_url from xcom
    headers, base_url = _get_base_url_headers_from_xcom(ti)

    api = "generate_report"
    print(f"Start making request to {api}")

    # Make post-request and raise HttpError in case of error 
    response = requests.post(f'{base_url}/{api}', headers=headers)
    response.raise_for_status()

    # Grab task_id from response and push it to xcom
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)

    print(f"Stop making request to {api}")


def get_base_url_and_headers(ti):
    # Get information about HTTP-connection via BaseHook
    http_conn_id = BaseHook.get_connection("http_conn_id")
    api_key, base_url = http_conn_id.extra_dejson.get("api_key"), http_conn_id.host

    # Build headers dictionary
    headers = {
      "X-Nickname": Variable.get("nickname"),
      "X-Project": 'True',
      "X-Cohort": Variable.get("cohort"),
      "X-API-KEY": api_key,
      "Content-Type": "application/x-www-form-urlencoded"
    }

    ti.xcom_push(key="headers", value=headers)
    ti.xcom_push(key="base_url", value=base_url)


args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(dag_id="sales_mart",
         default_args=args,
         description="Project for sprint3",
         catchup=False,
         schedule_interval="0 0 * * *",
         start_date=datetime(2021, 1, 1),
        ) as dag:
    t_get_base_url_and_headers = PythonOperator(
                                    task_id="t_get_base_url_and_headers",
                                    python_callable=get_base_url_and_headers
                                )
    
    t_create_task_for_report_generation = PythonOperator(
                                            task_id="t_create_task_for_report_generation",
                                            python_callable=create_task_for_report_generation
                                          )

    t_get_report_info = PythonOperator(
        task_id="t_get_report_info",
        python_callable=get_report_info
    )

    t_get_base_url_and_headers >> t_create_task_for_report_generation >> t_get_report_info

#business_dt = "{{ ds }}"
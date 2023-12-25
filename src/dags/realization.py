import requests
import json
import pandas as pd
import os
import time


from airflow.configuration import conf
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_base_url_headers_from_xcom(ti):
    return (ti.xcom_pull(key=key, task_ids="get_data_from_api_group.t_get_base_url_and_headers") for key in ("headers", "base_url"))


def _get_increment(base_url, headers, report_id, dt):
    response = requests.get(f"{base_url}/get_increment?report_id={report_id}&date={dt}T00:00:00", headers=headers)
    response.raise_for_status()
    rc = json.loads(response.content)
    status = rc["status"]

    if status == "SUCCESS":
        return status, rc["data"]["s3_path"]
    
    return status, None


def _check_file_was_uploaded(filename):
    with PostgresHook(postgres_conn_id="postgresql_de").get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM staging.start_staging_load(%s);", (filename,))
            records = cursor.fetchall()

            '''
            start_staging_load function always returns one row as file_id and upload_status
            if file was never uploaded or there was an error then id and "in_progress" status will be returned
            '''
            for record in records:
                if record[1] == 'success':
                    return record[0], True
                return record[0], False


def _update_loading_status(load_id, status):
    with PostgresHook(postgres_conn_id="postgresql_de").get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("CALL staging.finish_staging_load(%s, %s);", (load_id, status)) 


def _download_file(ti, key_for_s3_path, key_for_url, staging_folder, filename):
    s3_path = ti.xcom_pull(key=key_for_s3_path, task_ids="get_data_from_api_group.t_get_report_info")
    url, download_path = s3_path[key_for_url], os.path.join(staging_folder, filename)

    if not os.path.isfile(download_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(download_path, mode="wb") as file:
            file.write(response.content)
    
    return download_path


def _load_data_to_db(object_name, schema_name, download_path, extra_columns, unique_columns, add_columns, load_id):
    # Load data to DataFrame and get rid of extra columns and duplicates
    df = pd.read_csv(download_path)
    df.drop(extra_columns, axis=1, inplace=True)
    df.drop_duplicates(subset=unique_columns, inplace=True)
    
    # Set additional columns if required
    for k, v in add_columns.items():
        if k not in df.columns:
            df[k] = v

    # Load data to db
    engine = PostgresHook(postgres_conn_id="postgresql_de").get_sqlalchemy_engine()
    row_count = df.to_sql(object_name, engine, schema=schema_name, if_exists="append", index=False)
    _update_loading_status(load_id, "success")


def upload_data_to_staging(ti, object_name, dt, extra_columns, unique_columns, add_columns, schema_name):
    base_folder = os.path.dirname(conf.get("core", "dags_folder"))
    staging_folder = os.path.join(base_folder, schema_name)

    if dt is None or dt == "":
        # We need to upload full report
        key_for_s3_path, key_for_url, filename = "s3_path", object_name, f"{object_name}.csv"
    else:
        # We need to upload incremental report 
        key_for_s3_path, key_for_url, filename = "s3_inc_path", f"{object_name}_inc", f"{object_name}_{dt.replace('-', '_')}.csv"

    # Check if file has already been uploaded. If so, then exit
    load_id, was_uploaded = _check_file_was_uploaded(filename)
    if was_uploaded:
        return

    # If data was not found then update status in database and exit
    resp_status = ti.xcom_pull(key="resp_status", task_ids="get_data_from_api_group.t_get_report_info")
    if resp_status != "SUCCESS":
        _update_loading_status(load_id, ("failed", "not_found")[resp_status == "NOT FOUND"])
        return

    # Check if staging_folder exists. If not, create it
    if not os.path.isdir(staging_folder):
        os.mkdir(staging_folder)

    # Download file if it is not in staging directory
    download_path = _download_file(ti, key_for_s3_path, key_for_url, staging_folder, filename)

    # Load data to database
    _load_data_to_db(object_name, schema_name, download_path, extra_columns, unique_columns, add_columns, load_id)
    

def get_report_info(ti, dt=None):
    # Get headers, base_url and task_id from xcom
    headers, base_url = _get_base_url_headers_from_xcom(ti)
    task_id = ti.xcom_pull(key="task_id", task_ids="get_data_from_api_group.t_create_task_for_report_generation")
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
        rc = json.loads(response.content)
        status = rc["status"]
        print(f"{i} request status: {status}")

        if status == "SUCCESS":
            report_id = rc["data"]["report_id"]
            s3_path = rc["data"]["s3_path"]
            break

        time.sleep(10)

    if not report_id:
        raise TimeoutError()

    # If dt was specified then we need to save information about increment
    if dt is not None and dt != "":
        status, inc_data = _get_increment(base_url, headers, report_id, dt)

    ti.xcom_push(key="report_id", value=report_id)
    ti.xcom_push(key="s3_path", value=s3_path)
    ti.xcom_push(key="resp_status", value=status)   
    ti.xcom_push(key="s3_inc_path", value=inc_data)


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


business_dt = "2023-12-18"
staging_schema = "staging"

with DAG(dag_id="sales_mart",
         default_args=args,
         description="Project for sprint3",
         catchup=False,
         schedule_interval="0 0 * * *",
         start_date=datetime(2021, 1, 1),
        ) as dag:


    with TaskGroup("get_data_from_api_group") as get_data_from_api_group:
        t_get_base_url_and_headers = PythonOperator(task_id="t_get_base_url_and_headers", 
                                                    python_callable=get_base_url_and_headers)

        t_create_task_for_report_generation = PythonOperator(task_id="t_create_task_for_report_generation", 
                                                             python_callable=create_task_for_report_generation)

        t_get_report_info = PythonOperator(task_id="t_get_report_info", 
                                           python_callable=get_report_info,
                                           op_kwargs={"dt": business_dt})
        
        t_upload_customer_research_to_staging = PythonOperator(task_id="t_upload_customer_research_to_staging", 
                                                               python_callable=upload_data_to_staging,
                                                               op_kwargs={
                                                                   "object_name": "customer_research",
                                                                   "dt": business_dt,
                                                                   "extra_columns": [],
                                                                   "unique_columns": ["date_id", "category_id", "geo_id"],
                                                                   "add_columns": {},
                                                                   "schema_name":  staging_schema
                                                               }
                                                              )

        t_upload_user_activity_log_to_staging = PythonOperator(task_id="t_upload_user_activity_log_to_staging", 
                                                               python_callable=upload_data_to_staging,
                                                               op_kwargs={
                                                                   "object_name": "user_activity_log",
                                                                   "dt": business_dt,
                                                                   "extra_columns": ["id"],
                                                                   "unique_columns": ["uniq_id"],
                                                                   "add_columns": {},
                                                                   "schema_name":  staging_schema
                                                               }
                                                              )

        t_upload_user_order_log_to_staging = PythonOperator(task_id="t_upload_user_order_log_to_staging", 
                                                            python_callable=upload_data_to_staging,
                                                            op_kwargs={
                                                                   "object_name": "user_order_log",
                                                                   "dt": business_dt,
                                                                   "extra_columns": ["id"],
                                                                   "unique_columns": ["uniq_id"],
                                                                   "add_columns": {"status": "shipped"},
                                                                   "schema_name":  staging_schema
                                                            }
                                                           )

        t_get_base_url_and_headers >> t_create_task_for_report_generation >> t_get_report_info
        t_get_report_info >> [t_upload_customer_research_to_staging, t_upload_user_activity_log_to_staging, t_upload_user_order_log_to_staging]

    get_data_from_api_group
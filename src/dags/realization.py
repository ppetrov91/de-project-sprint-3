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
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_base_url_headers_from_xcom(ti):
    '''
    Get headers and base_url from xcom for performing GET and POST requests
    '''
    return (ti.xcom_pull(key=key, task_ids="download_data_to_staging.t_get_base_url_and_headers") for key in ("headers", "base_url"))


def _get_increment(base_url, headers, report_id, dt):
    '''
    Get info about incremental reports from API in a form of JSON and save it to xcom
    '''
    response = requests.get(f"{base_url}/get_increment?report_id={report_id}&date={dt}T00:00:00", headers=headers)
    response.raise_for_status()
    rc = json.loads(response.content)
    status = rc["status"]

    if status == "SUCCESS":
        return status, rc["data"]["s3_path"]
    
    return status, None


def _start_staging_load(filename):
    '''
    Add information in database that data loading has begun
    '''
    with PostgresHook(postgres_conn_id="postgresql_de").get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("CALL staging.start_staging_load(%s);", (filename,))


def _remove_data_from_prev_runs(file_name, obj_name, dt_col_name):
    '''
    Remove data from staging schema while reloading
    '''
    with PostgresHook(postgres_conn_id="postgresql_de").get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("CALL staging.drop_staging_file_data(%s, %s, %s);", (file_name, obj_name, dt_col_name)) 


def _finish_staging_load(filename, status, min_date=None, max_date=None):
    '''
    Add information in database that data loading has finished.
    Also update min_date and max_date which were calculated by analyzing pandas dataframe 
    '''
    with PostgresHook(postgres_conn_id="postgresql_de").get_conn() as conn:
        with conn.cursor() as cursor:
            print(f"filename = {filename}, status={status}")
            cursor.execute("CALL staging.finish_staging_load(%s, %s, %s, %s);", (filename, status, min_date, max_date)) 


def _download_file(ti, key_for_s3_path, key_for_url, staging_folder, filename):
    '''
    Download data file from the source system and return local filepath where data was downloaded
    '''
    s3_path = ti.xcom_pull(key=key_for_s3_path, task_ids="download_data_to_staging.t_get_report_info")
    url, download_path = s3_path[key_for_url], os.path.join(staging_folder, filename)

    response = requests.get(url)
    response.raise_for_status()
    with open(download_path, mode="wb") as file:
        file.write(response.content)
    
    return download_path


def _load_data_to_db(object_name, file_name, schema_name, download_path, extra_columns, unique_columns, add_columns, dt_col_name):
    '''
    This function reads data from csv, then remove extra columns and duplicates.
    Then it adds additional columns such as status in user_order_log if necessary.
    Then it uploads data to database and mark data loading as finished
    '''
    # Load data to DataFrame and get rid of extra columns and duplicates
    df = pd.read_csv(download_path)
    df.drop(extra_columns, axis=1, inplace=True)
    df.drop_duplicates(subset=unique_columns, inplace=True)
    
    # Set additional columns if required
    for k, v in add_columns.items():
        if k not in df.columns:
            df[k] = v

    # Load data to db
    min_date, max_date = df[dt_col_name].min(), df[dt_col_name].max()
    engine = PostgresHook(postgres_conn_id="postgresql_de").get_sqlalchemy_engine()
    row_count = df.to_sql(object_name, engine, schema=schema_name, if_exists="append", index=False)
    _finish_staging_load(file_name, "success", min_date, max_date)


def upload_data_to_staging(ti, object_name, dt, extra_columns, unique_columns, add_columns, schema_name, dt_col_name):
    '''
    Main function for uploading data to staging
    '''
    base_folder = os.path.dirname(conf.get("core", "dags_folder"))
    staging_folder = os.path.join(base_folder, schema_name)

    if dt is None or dt == "":
        # We need to upload full report
        key_for_s3_path, key_for_url, file_name = "s3_path", object_name, f"{object_name}.csv"
    else:
        # We need to upload incremental report 
        key_for_s3_path, key_for_url, file_name = "s3_inc_path", f"{object_name}_inc", f"{object_name}_{dt.replace('-', '_')}.csv"

    # Get min_date and max_date from previous load if any
    _start_staging_load(file_name)

    # If data was not found then update status in database and exit
    resp_status = ti.xcom_pull(key="resp_status", task_ids="download_data_to_staging.t_get_report_info")
    if resp_status != "SUCCESS":
        _finish_staging_load(file_name, ("failed", "not_found")[resp_status == "NOT FOUND"])
        return

    # Remove data from previous runs if any
    _remove_data_from_prev_runs(file_name, object_name, dt_col_name)

    # Check if staging_folder exists. If not, create it
    if not os.path.isdir(staging_folder):
        os.mkdir(staging_folder)

    # Download file if it is not in staging directory
    download_path = _download_file(ti, key_for_s3_path, key_for_url, staging_folder, file_name)

    # Load data to database
    _load_data_to_db(object_name, file_name, schema_name, download_path, extra_columns, unique_columns, add_columns, dt_col_name)
    

def get_report_info(ti, dt=None):
    '''
    Get info about report_id and location of main report files
    '''

    # Get headers, base_url and task_id from xcom
    headers, base_url = _get_base_url_headers_from_xcom(ti)
    task_id = ti.xcom_pull(key="task_id", task_ids="download_data_to_staging.t_create_task_for_report_generation")
    report_id, inc_data, api = None, None, "get_report"
    
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
    '''
    This function makes a POST-request to create a task for report generation
    '''

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
    '''
    Get info about headers and url of the source system
    '''

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


business_dt = "2023-12-19"
staging_schema = "staging"

with DAG(dag_id="sales_mart",
         default_args=args,
         description="Project for sprint3",
         catchup=False,
         schedule_interval="0 0 * * *",
         start_date=datetime(2021, 1, 1),
        ) as dag:


    with TaskGroup("download_data_to_staging") as download_data_to_staging:
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
                                                                   "schema_name":  staging_schema,
                                                                   "dt_col_name": "date_id"
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
                                                                   "schema_name":  staging_schema,
                                                                   "dt_col_name": "date_time"
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
                                                                   "schema_name":  staging_schema,
                                                                   "dt_col_name": "date_time"
                                                            }
                                                           )

        t_get_base_url_and_headers >> t_create_task_for_report_generation >> t_get_report_info
        t_get_report_info >> [t_upload_customer_research_to_staging, t_upload_user_activity_log_to_staging, t_upload_user_order_log_to_staging]

    download_data_to_staging
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, time, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG, AirflowException
import subprocess
import json
import boto3
import yaml
from yaml.loader import SafeLoader

START_DATE = datetime.now() - timedelta(minutes=1470)

CONFIG_PATH = "s3://movie-analysis-code-bucket/app/conf/config.yml"

EMR_IP_ADDRESS = ""

SSH_CONNECT = "ssh -i /usr/local/airflow/ssh/emr-kpx.pem hadoop@"

def path_to_bucket_key(path):
    path = path.split('/')
    bucket = path[2]
    key = '/'.join(path[3:])

    return bucket, key


def read_s3_file(path, encoding='utf8'):
    s3_client = boto3.client('s3')

    bucket, key = path_to_bucket_key(path)

    obj = s3_client.get_object(Bucket=bucket, Key=key)

    return obj['Body'].read().decode(encoding)


file = read_s3_file(CONFIG_PATH)

config = yaml.load(file, Loader=SafeLoader)

get_movie_data_script_path = config['dags']['get_movie_data_script_path']

data_cleaning_script_path = config['dags']['data_cleaning_script_path']

schema_creation_script_path = config['dags']['schema_creation_script_path']

data_quality_check_script_path = config['dags']['data_quality_check_script_path']

execute_get_movie_data_script_cmd = f"{SSH_CONNECT} {EMR_IP_ADDRESS} spark-submit main.py '{'job_name': get_movie_data_script_path, 'path':{CONFIG_PATH}}'"

execute_data_cleaning_script_cmd = f"{SSH_CONNECT} {EMR_IP_ADDRESS} spark-submit main.py '{'job_name': data_cleaning_script_path, 'path':{CONFIG_PATH}}'"

execute_schema_creation_script_cmd = f"{SSH_CONNECT} {EMR_IP_ADDRESS} spark-submit main.py '{'job_name': schema_creation_script_path, 'path':{CONFIG_PATH}}'"

execute_data_quality_check_script_cmd = f"{SSH_CONNECT} {EMR_IP_ADDRESS} spark-submit main.py '{'job_name': data_quality_check_script_path, 'path':{CONFIG_PATH}}'"

    
default_args = {

    'owner': "Tanvi",
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'end_date': datetime(2099,12,31),
    'retry_delay': timedelta(minutes=1),
    'retries': 0,
    'concurrency': 1

}

dag_obj = DAG('get_daily_data_dag', max_active_runs=1, schedule_interval="0 3 * * 2-6", catchup=False, default_args=default_args)

start_task = DummyOperator(task_id = "start", dag = dag_obj)

get_movie_data_task = BashOperator(task_id="get_movie_data", bash_command = execute_get_movie_data_script_cmd, dag = dag_obj)

data_cleaning_task = BashOperator(task_id="data_cleaning", bash_command = execute_data_cleaning_script_cmd, dag = dag_obj)

schema_creation_task = BashOperator(task_id="schema_creation", bash_command = execute_schema_creation_script_cmd, dag = dag_obj)

data_quality_check_task = BashOperator(task_id="data_quality_check", bash_command = execute_data_quality_check_script_cmd, dag = dag_obj)

end_task = DummyOperator(task_id="end", dag=dag_obj, trigger_rule=TriggerRule.ONE_SUCCESS)

start_task >> get_movie_data_task >> data_cleaning_task >> schema_creation_task >> data_quality_check_task >> end_task
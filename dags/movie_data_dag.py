from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, time, timedelta
from airflow import DAG, AirflowException
import subprocess
import json
import boto3
import yaml
from yaml.loader import SafeLoader
import os 

START_DATE = datetime.now() - timedelta(minutes=1470)

CONFIG_PATH = "s3://movie-analysis-code-bucket/Movie_Analysis/app/conf/config.yml"

EMR_IP_ADDRESS = "ec2-44-192-89-241.compute-1.amazonaws.com"

SSH_CONNECT = "ssh -i C:/Users/DELL/airflow/airflow-tutorial/emr-kpx.pem hadoop@"

ACCESS_KEY = "" 

SECRET_KEY = ""

REGION_NAME = "us-east-1"


def path_to_bucket_key(path):
    path = path.split('/')
    bucket = path[2]
    key = '/'.join(path[3:])

    return bucket, key


def read_s3_file(path, encoding='utf8'):
    s3_client = boto3.client('s3',
            region_name=REGION_NAME,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key= SECRET_KEY)

    bucket, key = path_to_bucket_key(path)

    obj = s3_client.get_object(Bucket=bucket, Key=key)

    print("CWD is", os.getcwd())
    print("ALL DIRS", os.listdir())

    return obj['Body'].read().decode(encoding)


file = read_s3_file(CONFIG_PATH)

config = yaml.load(file, Loader=SafeLoader)

get_movie_data_script_path = config['dags']['get_movie_data_script_path']

data_cleaning_script_path = config['dags']['data_cleaning_script_path']

schema_creation_script_path = config['dags']['schema_creation_script_path']

data_quality_check_script_path = config['dags']['data_quality_check_script_path']

sentiment_analysis_script_path = config['dags']['data_quality_check_script_path']

main_path = config['paths']['main_path']

execute_get_movie_data_script_cmd = f"{SSH_CONNECT}{EMR_IP_ADDRESS} spark-submit {main_path}'{{'job_name': '{get_movie_data_script_path}', 'path': '{CONFIG_PATH}'}}'"

execute_data_cleaning_script_cmd = f"{SSH_CONNECT}{EMR_IP_ADDRESS} spark-submit {main_path} '{{'job_name': '{data_cleaning_script_path}', 'path': '{CONFIG_PATH}'}}'"

execute_schema_creation_script_cmd = f"{SSH_CONNECT}{EMR_IP_ADDRESS} spark-submit {main_path} '{{'job_name': '{schema_creation_script_path}', 'path': '{CONFIG_PATH}'}}'"

execute_data_quality_check_script_cmd = f"{SSH_CONNECT}{EMR_IP_ADDRESS} spark-submit {main_path} '{{'job_name': '{data_quality_check_script_path}', 'path': '{CONFIG_PATH}'}}'"

execute_sentiment_analysis_script_cmd = f"{SSH_CONNECT}{EMR_IP_ADDRESS} spark-submit {main_path} '{{'job_name': '{sentiment_analysis_script_path}', 'path': '{CONFIG_PATH}'}}'"
    
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

dag_obj = DAG('movie_data_dag', max_active_runs=1, schedule_interval="0 10 * * FRI", catchup=False, default_args=default_args)

start_task = DummyOperator(task_id = "start", dag = dag_obj)

get_movie_data_task = BashOperator(task_id="get_movie_data", bash_command = execute_get_movie_data_script_cmd, dag = dag_obj)

data_cleaning_task = BashOperator(task_id="data_cleaning", bash_command = execute_data_cleaning_script_cmd, dag = dag_obj)

schema_creation_task = BashOperator(task_id="schema_creation", bash_command = execute_schema_creation_script_cmd, dag = dag_obj)

data_quality_check_task = BashOperator(task_id="data_quality_check", bash_command = execute_data_quality_check_script_cmd, dag = dag_obj)

sentiment_analysis_task = BashOperator(task_id="sentiment_analysis", bash_command = execute_sentiment_analysis_script_cmd, dag = dag_obj)

end_task = DummyOperator(task_id="end", dag=dag_obj)

start_task >> get_movie_data_task >> data_cleaning_task >> schema_creation_task >> data_quality_check_task >> sentiment_analysis_task >> end_task
'''
----------------------------------------------
Project: Movie Analysis
File: movie_data_dag.py
Description:
    
    ---DAG which runs from Tuesday to Saturday
    at 8 AM IST in order to extracts data from
    NSE website---
    
-----------------------------------------------
'''

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, time, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG, AirflowException
import subprocess
import json

START_DATE = datetime.now() - timedelta(minutes=1470)

# with open('config.json') as file:
#   config_data = json.load(file)

get_movie_data_script_path = config['dags']['get_movie_data_script_path']

data_cleaning_script_path = config['dags']['data_cleaning_script_path']

schema_creation_script_path = config['dags']['schema_creation_script_path']

data_quality_check_script_path = config['dags']['data_quality_check_script_path']

config_path = config['dags']['config_path']

# execute_holiday_script_cmd = f"python {holiday_script_path}"

execute_get_movie_data_script_cmd = f"spark-submit main.py '{'job_name': get_movie_data_script_path, 'path':{config_path}}'"

execute_data_cleaning_script_cmd = f"spark-submit main.py '{'job_name': data_cleaning_script_path, 'path':{config_path}}'"

execute_schema_creation_script_cmd = f"spark-submit main.py '{'job_name': schema_creation_script_path, 'path':{config_path}}'"

execute_data_quality_check_script_cmd = f"spark-submit main.py '{'job_name': data_quality_check_script_path, 'path':{config_path}}'"


# def validate_day():
#   output = subprocess.call(execute_holiday_script_cmd)

#   if output != 0:
#     print("Invalid day to run DAG")
#     return "invalid_day"

#   else:
#     print("current_data script is scheduled")
#     return "run_script"


def data_quality_check():
   output = subprocess.call(execute_data_quality_check_script_cmd)

   if output != 0:
     raise Exception("Data Quality Check has failed")

   else:
     print("Data Quality Check has been successful")

    
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

# validate_day_task = BranchPythonOperator(task_id="validate_day", python_callable = validate_day, dag = dag_obj)

# run_script = BashOperator(task_id="run_script", bash_command = execute_current_script_cmd, dag = dag_obj)
get_movie_data_task = BashOperator(task_id="get_movie_data", bash_command = execute_get_movie_data_script_cmd, dag = dag_obj)

data_cleaning_task = BashOperator(task_id="data_cleaning", bash_command = execute_data_cleaning_script_cmd, dag = dag_obj)

schema_creation_task = BashOperator(task_id="schema_creation", bash_command = execute_schema_creation_script_cmd, dag = dag_obj)

data_quality_check_task = BashOperator(task_id="data_quality_check", bash_command = execute_data_quality_check_script_cmd, dag = dag_obj)

dq_check = BashOperator(task_id="data_quality_check", python_callable=data_quality_check, dag=dag_obj)

# invalid_day = DummyOperator(task_id="invalid_day", dag=dag_obj)

end_task = DummyOperator(task_id="end", dag=dag_obj, trigger_rule=TriggerRule.ONE_SUCCESS)

start_task >> get_movie_data_task >> data_cleaning_task >> schema_creation_task >> data_quality_check_task >> end_task
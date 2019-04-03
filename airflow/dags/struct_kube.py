from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'struct_dag', default_args=default_args, schedule_interval=timedelta(minutes=10))



t1 = BashOperator(
    task_id='struct',
    bash_command='kubectl apply -f /home/ubuntu/kube/struct.yaml',
    dag=dag)


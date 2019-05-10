from __future__ import print_function
from builtins import range
from airflow.operators import PythonOperator
from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import subprocess
from subprocess import PIPE
import redis
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': datetime.utcnow(),
    'start_date': datetime(2017, 9, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'bwa_gatk_dag_sleek', default_args=default_args, schedule_interval=None)

r = redis.Redis(host='172.31.45.104')

def waitfor_bwa(run_id='runid2', task_id='taskid1'):
    update_key = run_id + '.' + task_id
    while not r.get(update_key):
        print ("Job in progress")
        time.sleep(30)
    else:
        print ("Completed task")


def waitfor_gatk(run_id='runid2', task_id='taskid2'):
    update_key = run_id + '.' + task_id
    while not r.get(update_key):
        print ("Job in progress")
        time.sleep(30)
    else:
        print ("Completed task GATK")


t1 = BashOperator(
        task_id='bwa_cc',
        bash_command='export KUBECONFIG=/root/.kube/kind-config-kind && kubectl apply -f /tmp/bwapod.yaml',
        dag=dag)
t2 = PythonOperator(
        task_id= 'bwa_wait',
        python_callable=waitfor_bwa,
        op_kwargs={},
        dag=dag)
t3 = BashOperator(
        task_id='gatk_cc',
        bash_command='export KUBECONFIG=/root/.kube/kind-config-kind && kubectl apply -f /tmp/gatkpod.yaml',
        dag=dag)
t4 = PythonOperator(
        task_id= 'gatk_wait',
        python_callable=waitfor_gatk,
        op_kwargs={},
        dag=dag)

t1>>t2>>t3>>t4

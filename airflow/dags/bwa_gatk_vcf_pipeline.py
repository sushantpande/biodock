from __future__ import print_function
from builtins import range
from airflow.operators import PythonOperator
from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import subprocess
from subprocess import PIPE
import subprocess
import redis
import time
import yaml
from airflow.models import Variable
import os
import hashlib

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': datetime.utcnow(),
    'start_date': datetime(2017, 9, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bwa_gatk_dag_sleek', default_args=default_args, schedule_interval=None)


WORK_DIR = Variable.get('WORK_DIR_NAME')
AMQP_SERVER = Variable.get('AMQP_SERVER')
AMQP_PORT = Variable.get('AMQP_PORT')
AMQP_USER = Variable.get('AMQP_USER')
AMQP_PWD = Variable.get('AMQP_PWD')
REDIS_HOST = Variable.get('REDIS_HOST')
DATA_DIR = Variable.get('bwa_gatk_dag_sleek.DATA_DIR')
REF_FILE = Variable.get('bwa_gatk_dag_sleek.ref')
BWA_INPUT = Variable.get('bwa_gatk_dag_sleek.bwa.input')
BWA_SPLIT = Variable.get('bwa_gatk_dag_sleek.bwa.split')
BWA_SPLIT_SIZE = Variable.get('bwa_gatk_dag_sleek.bwa.split_size')
BWA_YAML = Variable.get('bwa_gatk_dag_sleek.bwa.yaml')
GATK_YAML = Variable.get('bwa_gatk_dag_sleek.gatk.yaml')
VCF_YAML = Variable.get('bwa_gatk_dag_sleek.vcf.yaml')
MAX_CONT = int(Variable.get('bwa_gatk_dag_sleek.max_cont'))


r = redis.Redis(host='172.31.45.104')

def create_container(yaml_file, **kwargs):
    parallelism = 0
    context = kwargs
    run_id = context['dag_run'].run_id
    task_id = context['task'].task_id
    parent_ids = context['task'].upstream_task_ids
    ti = context['ti']

    yaml_file_name = os.path.basename(yaml_file)
    this_work_dir = os.path.join(DATA_DIR, WORK_DIR, run_id)

    try:
        os.makedirs(this_work_dir)
    except FileExistsError:
        pass
 
    tmp_yaml_file = os.path.join(this_work_dir, yaml_file_name)
    for id in parent_ids:
        parallelism = parallelism + int(ti.xcom_pull(key=None, task_ids=id))

    if parallelism == 0:
        parallelism = 1
    else:
        if parallelism > MAX_CONT:
            parallelism = MAX_CONT 

    print (run_id)
    print (task_id)

    with open(yaml_file) as fh:
        manifest = yaml.load(fh)
    
    manifest['spec']['parallelism'] = parallelism 

    hash_object = hashlib.md5(run_id.encode())
    app_name = ("%s-%s" %(manifest['metadata']['name'], hash_object.hexdigest()))

    manifest['metadata']['name'] = app_name 

    for k in manifest['spec']['template']['spec']['containers'][0].keys():
        if k == 'env':
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'REDIS_HOST', 'value': REDIS_HOST})
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'AMQP_SERVER', 'value': AMQP_SERVER})
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'AMQP_PORT', 'value': AMQP_PORT})
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'AMQP_USER', 'value': AMQP_USER})
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'AMQP_PWD', 'value': AMQP_PWD})
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'RUN_ID', 'value': run_id})
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'TASK_ID', 'value': task_id})
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'DATA_DIR', 'value': DATA_DIR})
            manifest['spec']['template']['spec']['containers'][0][k].append({'name':'REF_FILE', 'value':REF_FILE})
            if task_id == 'bwa_cc':
                manifest['spec']['template']['spec']['containers'][0][k].append({'name':'INPUT_FILE', 'value':BWA_INPUT})
                manifest['spec']['template']['spec']['containers'][0][k].append({'name':'SPLIT', 'value':BWA_SPLIT})
                manifest['spec']['template']['spec']['containers'][0][k].append({'name':'SPLIT_SIZE', 'value':BWA_SPLIT_SIZE})
            break

    with open(tmp_yaml_file, 'w+') as fh:
        yaml.dump(manifest, fh, default_flow_style=False)

    cmd = "export KUBECONFIG=/root/.kube/kind-config-kind && kubectl apply -f " + tmp_yaml_file 
    completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE) 
    print ("Container creation: %s" %(completedProc.stderr))
    return (tmp_yaml_file)

def noop(**kwargs):
    context = kwargs
    run_id = context['dag_run'].run_id
    print ("Run Id: " + run_id)
    parent_ids = context['task'].upstream_task_ids
    wait_keys = []
    _wait_keys = []
    delete_jobs = []
    ti = context['ti']
    for id in parent_ids:
        key = run_id + "." + id
        _wait_keys.append(key)
        wait_keys.append(key)
        delete_jobs.append(ti.xcom_pull(key=None, task_ids=id))
    print (wait_keys)

    while (_wait_keys):
        time.sleep(30)
        print ("Jobs are in progress.")
        for key in wait_keys:
            val = r.get(key)
            print ("Key %s, Val %s" %(key, val))
            if val != None:
                _wait_keys.remove(key)

    split_count = 0
    for id in parent_ids:
        key = run_id + "." + id + "." + 'split'
        val = r.get(key)
        if val:
             split_count = split_count + int(val)

    print ("split_count: " + str(split_count))

    for job in delete_jobs:
       cmd = "export KUBECONFIG=/root/.kube/kind-config-kind && kubectl delete -f " + job
       #completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
    return (split_count)

t1 = PythonOperator(
        task_id= 'bwa_cc',
        python_callable=create_container,
        op_kwargs={'yaml_file': BWA_YAML},
        provide_context=True,
        dag=dag)

t2 = PythonOperator(
        task_id= 'bwa_wait',
        python_callable=noop,
        op_kwargs={},
        provide_context=True,
        dag=dag)

t4 = PythonOperator(
        task_id= 'gatk_cc',
        python_callable=create_container,
        op_kwargs={'yaml_file':GATK_YAML},
        provide_context=True,
        dag=dag)

t5 = PythonOperator(
        task_id= 'gatk_wait',
        python_callable=noop,
        op_kwargs={},
        provide_context=True,
        dag=dag)

t7 = PythonOperator(
        task_id= 'vcf_cc',
        python_callable=create_container,
        op_kwargs={'yaml_file':VCF_YAML},
        provide_context=True,
        dag=dag)

t9 = PythonOperator(
        task_id= 'vcf_wait',
        python_callable=noop,
        op_kwargs={},
        provide_context=True,
        dag=dag)

#t1>>t2>>t3>>t4>>t5>>t6
t1>>t2>>t4>>t5>>t7>>t9

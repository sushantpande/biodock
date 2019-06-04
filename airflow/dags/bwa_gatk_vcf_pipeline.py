from __future__ import print_function
from builtins import range
from airflow.operators import PythonOperator
from airflow.operators import BashOperator
from airflow.models import DAG, Variable
from datetime import datetime, timedelta
import subprocess
from subprocess import PIPE
import time
import yaml
import os
import re
import redis
import hashlib
from deploy_jobs import kube_create_job_object, kube_run_job_object, kube_monitor_job_status, kube_cleanup_complete_jobs

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

dag_id = "vcfp"
dag = DAG(
    dag_id, default_args=default_args, schedule_interval=None)


def get_dag_var(key, default_var=None):
    if default_var == None:
        return Variable.get(key)
    else:
        return Variable.get(key, default_var=default_var)

AMQP_SERVER = get_dag_var('AMQP_SERVER')
AMQP_PORT = get_dag_var('AMQP_PORT')
AMQP_USER = get_dag_var('AMQP_USER')
AMQP_PWD = get_dag_var('AMQP_PWD')
REDIS_HOST = get_dag_var('REDIS_HOST')

key = ("%s.%s" %(dag_id, 'DATA_DIR'))
DATA_DIR = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'ref'))
REF_FILE = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'bwa.input'))
BWA_INPUT = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'bwa.split_output'))
BWA_SPLIT_OUTPUT = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'bwa.split_output_size'))
BWA_SPLIT_OUTPUT_SIZE = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'bwa.image'))
BWA_IMAGE = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'gatk.image'))
GATK_IMAGE = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'vcf.image'))
VCF_IMAGE = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'max_cont'))
MAX_CONT = int(get_dag_var(key))

key = ("%s.%s" %(dag_id, 'bwa.cmd'))
BWA_CMD = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'gatk.cmd'))
GATK_CMD = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'vcf.cmd'))
VCF_CMD = get_dag_var(key)

key = ("%s.%s" %(dag_id, 'job.queue'))
JOB_QUEUE = get_dag_var(key, default_var='')

key = ("%s.%s" %(dag_id, 'work_dir'))
WORK_DIR = get_dag_var(key)

redis_conn = None
try:
    redis_conn = redis.Redis(host=REDIS_HOST)
except:
    print ("Failed to open connection to redis server (%s)" %(host))
 

def create_container(container_image, cmd, **kwargs):
    parallelism = 0
    env_vars = {} 
    context = kwargs
    run_id = context['dag_run'].run_id
    task_id = context['task'].task_id
    parent_ids = context['task'].upstream_task_ids
    ti = context['ti']

    print ("Run Id is (%s) and Task Id is (%s)" %(run_id, task_id))

    '''Number of containers to spin for a job'''
    for id in parent_ids:
        parallelism = parallelism + int(ti.xcom_pull(key='parallelism', task_ids=id))

    if parallelism == 0:
        parallelism = 1
    else:
        if parallelism > MAX_CONT:
            parallelism = MAX_CONT 

    '''Set the queue name for downstream jobs'''
    out_queue = ("%s.%s" %(run_id, task_id)) 

    '''Set the input queue for this job'''
    in_queue = ""

    if not parent_ids:
        in_queue = JOB_QUEUE
    else:
        for id in parent_ids:
            in_queue = ("%s %s " %(in_queue, ti.xcom_pull(key='queue', task_ids=id)))

    '''Get Job and container name (not more then 63 chars)'''
    hash_object = hashlib.md5(run_id.encode())
    container_name = "-".join(re.split('\W+', container_image))
    jobname = ("%s-%s" %(container_name, hash_object.hexdigest()))[:63]

    '''Build evn for job containers'''
    env_vars.update({'REDIS_HOST': REDIS_HOST})
    env_vars.update({'AMQP_SERVER': AMQP_SERVER}) 
    env_vars.update({'AMQP_PORT': AMQP_PORT}) 
    env_vars.update({'AMQP_USER': AMQP_USER}) 
    env_vars.update({'AMQP_PWD': AMQP_PWD}) 
    env_vars.update({'RUN_ID': run_id})
    env_vars.update({'TASK_ID': task_id})
    env_vars.update({'DATA_DIR': DATA_DIR})
    env_vars.update({'WORK_DIR': WORK_DIR})
    env_vars.update({'REF_FILE': REF_FILE})
    env_vars.update({'IN_QUEUE': in_queue})
    env_vars.update({'OUT_QUEUE': out_queue})

    if task_id == 'bwa':
        env_vars.update({'bwa_INPUT': BWA_INPUT})
        env_vars.update({'BWA_SPLIT_OUTPUT': BWA_SPLIT_OUTPUT})
        env_vars.update({'BWA_SPLIT_OUTPUT_SIZE': BWA_SPLIT_OUTPUT_SIZE})

    '''Let us use default namespace for now'''
    namespace = "default"

    '''Create, deploy and monitor job'''
    job = kube_create_job_object(jobname, container_image, container_name, namespace, env_vars, parallelism, cmd) 
    kube_run_job_object(namespace, job)
    kube_monitor_job_status(namespace, jobname)

    '''Dump generated manifest file'''
    print ("Prepare to dump manifest file")
    this_work_dir = os.path.join(WORK_DIR, run_id, task_id)

    try:
        os.makedirs(this_work_dir)
    except FileExistsError:
        pass
    
    tmp_manifest_file = os.path.join(this_work_dir, jobname)

    try:
        with open(tmp_manifest_file, 'w+') as fh:
            yaml.dump(job, fh, default_flow_style=False)
            print ("Dumped manifest file (%s)" %(tmp_manifest_file))
    except IOError as e:
            print("Manifest file dumping for job (%s) failed with exception (%s)" %(jobname, e))

    '''Communicate to downstream few values'''
    ti.xcom_push(key='namespace', value=namespace)
    ti.xcom_push(key='jobname', value=jobname)
    ti.xcom_push(key='queue', value=out_queue)


def cleanup(**kwargs):
    context = kwargs
    run_id = context['dag_run'].run_id
    print ("Run Id: " + run_id)
    parent_ids = context['task'].upstream_task_ids
    ti = context['ti']
    for id in parent_ids:
        key = run_id + "." + id
        namespace = ti.xcom_pull(key='namespace', task_ids=id)
        jobname = ti.xcom_pull(key='jobname', task_ids=id)
        print ("Job (%s) is to be deleted from namespace (%s)" %(jobname, namespace))
        #if (namespace and jobname):
            #kube_cleanup_complete_jobs(jobname, namespace)
            

def get_output_file_count(run_id, task_id):
    count = 0
    file = os.path.join(WORK_DIR, run_id, task_id, ".meta")
    if os.path.exists(file):
        with open(file) as fh:
            try:
                meta = yaml.safe_load(fh)
                count = meta.get('OUTPUT_FILE_COUNT', 0)
            except yaml.YAMLError as e:
                print ("Failed to load yaml file (%s) got exception (%s)" %(file, e))
    print ("Output file count of task (%s) is (%s)" %(task_id, count))
    return count


def noop(**kwargs):
    context = kwargs
    run_id = context['dag_run'].run_id
    print ("Run Id: (%s)" %(run_id))
    parent_ids = context['task'].upstream_task_ids
    ti = context['ti']

    '''Get the total number of output files generated by previous stage'''
    '''This is to set the parallelism for next stage'''
    output_file_count = 0

    if redis_conn:
        for id in parent_ids:
            key = ("%s.%s.%s" %(run_id, id, 'OUTPUT_FILE_COUNT')) 
            val = redis_conn.get(key)
            if val:
                output_file_count = output_file_count + int(val)

    print ("output_file_count: (%s)" %(output_file_count))
    
    '''For file based update'''
    #for id in parent_ids:
    #    output_file_count = output_file_count + get_output_file_count(run_id, id)

    in_queue = ""
    if not parent_ids:
        in_queue = JOB_QUEUE
    else:
        for id in parent_ids:
            in_queue = ("%s %s " %(in_queue, ti.xcom_pull(key='queue', task_ids=id)))

    ti.xcom_push(key='queue', value=in_queue)
    ti.xcom_push(key='parallelism', value=output_file_count)


t1 = PythonOperator(
        task_id= 'bwa',
        python_callable=create_container,
        op_kwargs={'container_image': BWA_IMAGE, 'cmd': BWA_CMD},
        provide_context=True,
        dag=dag)

t2 = PythonOperator(
        task_id= 'bwa_wait',
        python_callable=noop,
        op_kwargs={},
        provide_context=True,
        dag=dag)

t3 = PythonOperator(
        task_id= 'bwa_cleanup',
        python_callable=cleanup,
        op_kwargs={},
        provide_context=True,
        dag=dag)

t4 = PythonOperator(
        task_id= 'gatk',
        python_callable=create_container,
        op_kwargs={'container_image':GATK_IMAGE, 'cmd': GATK_CMD },
        provide_context=True,
        dag=dag)

t5 = PythonOperator(
        task_id= 'gatk_wait',
        python_callable=noop,
        op_kwargs={},
        provide_context=True,
        dag=dag)

t6 = PythonOperator(
        task_id= 'gatk_cleanup',
        python_callable=cleanup,
        op_kwargs={},
        provide_context=True,
        dag=dag)

t7 = PythonOperator(
        task_id= 'vcf',
        python_callable=create_container,
        op_kwargs={'container_image':VCF_IMAGE, 'cmd': VCF_CMD},
        provide_context=True,
        dag=dag)

t8 = PythonOperator(
        task_id= 'vcf_wait',
        python_callable=noop,
        op_kwargs={},
        provide_context=True,
        dag=dag)

t9 = PythonOperator(
        task_id= 'vcf_cleanup',
        python_callable=cleanup,
        op_kwargs={},
        provide_context=True,
        dag=dag)

t1.set_downstream(t2)
t1.set_downstream(t3)
t2.set_downstream(t4)
t4.set_downstream(t5)
t4.set_downstream(t6)
t5.set_downstream(t7)
t7.set_downstream(t8)
t7.set_downstream(t9

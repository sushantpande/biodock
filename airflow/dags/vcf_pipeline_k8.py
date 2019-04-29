from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

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
    'bwa_gatk_dag', default_args=default_args, schedule_interval=None)


t1 = BashOperator(
    task_id='bwacreate',
    bash_command='export KUBECONFIG=/root/.kube/kind-config-kind && kubectl apply -f /tmp/bwapod.yaml',
    dag=dag)

t2 = BashOperator(
    task_id='bwaexec',
    bash_command='export KUBECONFIG=/root/.kube/kind-config-kind && POD=$(kubectl get pod -l app=bwaapp -o jsonpath="{.items[0].metadata.name}") && kubectl exec $POD -- /bin/bash -c "bwa mem -R {{ params.meta }} /tmp/scaffolds.fasta /tmp/evolved-6-R1.fastq | samtools sort > /tmp/bwaoutput.bam && samtools index /tmp/bwaoutput.bam && exit" && kubectl delete -n default deployment bwa-deployment',
    params={"meta": "'@RG\\tID:foo\\tLB:bar\\tPL:illumina\\tPU:illumina\\tSM:SAMPLE'"},
    dag=dag)

t3 = BashOperator(
    task_id='gatkcreate',
    bash_command='export KUBECONFIG=/root/.kube/kind-config-kind && kubectl apply -f /tmp/gatkpod.yaml',
    dag=dag)

t4 = BashOperator(
    task_id='gatkexec',
    bash_command='export KUBECONFIG=/root/.kube/kind-config-kind && POD=$(kubectl get pod -l app=gatkapp -o jsonpath="{.items[0].metadata.name}") && kubectl exec $POD -- /bin/bash -c "gatk HaplotypeCaller -R /tmp/scaffolds.fasta -I /tmp/bwaoutput.bam -O /tmp/gatkoutput.vcf" && kubectl delete -n default deployment gatk-deployment',
    dag=dag)


t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)

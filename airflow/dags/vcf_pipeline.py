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
    'bwa_gatk_dag', default_args=default_args, schedule_interval=timedelta(minutes=10))

t1 = BashOperator(
    task_id='bwa',
    bash_command='docker run -v /tmp:/tmp:rw bwa:v1.0.0 /bin/bash -c "bwa mem -R {{ params.meta }} /tmp/scaffolds.fasta /tmp/evolved-6-R1.fastq | samtools sort > /tmp/bwaoutput.bam && samtools index /tmp/bwaoutput.bam"',
    params={"meta": "'@RG\\tID:foo\\tLB:bar\\tPL:illumina\\tPU:illumina\\tSM:SAMPLE'"},
    dag=dag)


t2 = BashOperator(
    task_id='gatk',
    bash_command='docker run -v /tmp:/tmp:rw broadinstitute/gatk /bin/bash -c "gatk HaplotypeCaller -R /tmp/scaffolds.fasta -I /tmp/bwaoutput.bam -O /tmp/gatkoutput.vcf"',
    dag=dag)


t2.set_upstream(t1)

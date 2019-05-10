import subprocess
from subprocess import PIPE
import redis

completedProc = subprocess.run(['bwa mem -R "@RG\\tID:foo\\tLB:bar\\tPL:illumina\\tPU:illumina\\tSM:SAMPLE" /mnt/efs/scaffolds.fasta /mnt/efs/evolved-6-R1.fastq | samtools sort > /mnt/efs/bwaoutput.bam && samtools index /mnt/efs/bwaoutput.bam', "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)

print ('BWA container creation completed with status code:' + ' ' + str(completedProc.returncode))

r = redis.Redis(host='172.31.45.104')
r.set('runid2.taskid1', completedProc.returncode)

import subprocess
from subprocess import PIPE
import redis

completedProc = subprocess.run(['gatk HaplotypeCaller -R /mnt/efs/scaffolds.fasta -I /mnt/efs/bwaoutput.bam -O /mnt/efs/gatkoutput.vcf', "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)

print ('GATK container creation completed with status code:' + ' ' + str(completedProc.returncode))

r = redis.Redis(host='172.31.45.104')
r.set('runid2.taskid2', completedProc.returncode)

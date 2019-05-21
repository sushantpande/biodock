import subprocess
from subprocess import PIPE
import redis
import os
import pysam
import pika
import time
import glob

REDIS_HOST = os.environ['REDIS_HOST']
TASK_ID = os.environ['TASK_ID']
RUN_ID = os.environ['RUN_ID']
DATA_DIR = os.environ['DATA_DIR']
AMQP_SERVER = os.environ['AMQP_SERVER']
AMQP_PORT = os.environ['AMQP_PORT']
AMQP_USER = os.environ['AMQP_USER']
AMQP_PWD = os.environ['AMQP_PWD']
INPUT_FILE = os.environ['INPUT_FILE']
REFFILE = os.environ['REF_FILE']
split = int(os.environ['SPLIT'])
split_size = int(os.environ['SPLIT_SIZE'])

print  ('REDIS_HOST: ' +  REDIS_HOST)
print  ('TASK_ID: ' + TASK_ID)
print  ('RUN_ID: ' + RUN_ID)

input = os.path.join(DATA_DIR, INPUT_FILE)
output_dir = os.path.join(DATA_DIR, RUN_ID, TASK_ID)
output = os.path.join(output_dir, 'bwaoutput.bam')
ref = os.path.join(DATA_DIR, REFFILE)
print  ('INPUT: ' + input)
print  ('OUTPUT: ' + output)
print  ('REF: ' + ref)

try:
    os.makedirs(output_dir)
except FileExistsError:
    pass

cmd = 'bwa mem -R "@RG\\tID:foo\\tLB:bar\\tPL:illumina\\tPU:illumina\\tSM:SAMPLE" ' + ref + ' ' +\
      input + ' | samtools sort > ' + output + ' && samtools index ' + output

completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)

print ('BWA container creation completed with status code:' + ' ' + str(completedProc.returncode))



#Update jobs in the pipeline

credentials = pika.PlainCredentials(AMQP_USER, AMQP_PWD)
parameters = pika.ConnectionParameters(AMQP_SERVER, AMQP_PORT, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
queue = ("%s.%s" %(RUN_ID, TASK_ID))
channel.queue_declare(queue=queue)

split_count = 0

if split == 1:
    split_output_dir = os.path.join(DATA_DIR, RUN_ID, TASK_ID, "split")

    try:
        os.makedirs(split_output_dir)
    except FileExistsError:
        pass

    if split_size:
        lc = 500000
        header_file = os.path.join(split_output_dir, "header")
        glob_split_output_dir = os.path.join(split_output_dir, "*")
        cmd = "samtools view -H " + output + " > " + header_file
        completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
        split_prefix = os.path.join(split_output_dir,"bwaoutput")
        cmd = "samtools view " + output + " | split - " + split_prefix + " -l " + str(lc) + " --filter='cat " + header_file + " - | samtools view -b - > $FILE.bam' && rm " + header_file + " && samtools index " + glob_split_output_dir      
        completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
        file_list = glob.glob(glob_split_output_dir)
        for file in file_list:
            channel.basic_publish(exchange='', routing_key=queue, body=file)
        split_count = len(file_list)
    else:    
        cmd = "samtools view -H" + " " + output + " | cut -f2 | grep '^SN:' | sed s'/SN://'"
        completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
        samfile = pysam.AlignmentFile(output, "rb")

        for chr in (completedProc.stdout.decode('utf-8').split("\n")):
            if not chr:
                continue
            print ("Fetching chr: " + chr)
            reads = samfile.fetch(chr)
            outfile = None
            if reads:
                filename = chr + "_split.bam"
                filepath = os.path.join(output_dir, filename)
                outfile = pysam.AlignmentFile(filepath, "wb", template=samfile)
            for read in reads:
                outfile.write(read)
            outfile.close()
            pysam.index(filepath)
            channel.basic_publish(exchange='', routing_key=queue, body=filepath)
            print(" [x] Sent %r" % filepath)
            split_count = split_count + 1

        samfile.close()
else:
    split_count = 1
    channel.basic_publish(exchange='', routing_key=queue, body=output)

#while(True):
#    time.sleep(300)


r = redis.Redis(host=REDIS_HOST)
update_key = RUN_ID + "." + TASK_ID
r.set(update_key, completedProc.returncode)
print ('updated key: ' + update_key)
split_key = update_key + ".split"
r.set(split_key, split_count)
print ('updated key: ' + split_key)
queue_key = ("%s.queue" %(RUN_ID))
r.set(queue_key, queue) 
print ('Queue is: %s' %(queue))

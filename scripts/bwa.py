import subprocess
from subprocess import PIPE
import redis
import os
import pysam
import pika
import time

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
    if split_size:
        max_files = 1
        size = 100
        firstn = size
        samfile = pysam.AlignmentFile(output, "rb")
        reads = samfile.fetch()
        count = 0

        for read in reads:
            filename = "bwaoutput" + str(split_count) + "_split.sam"
            filepath = os.path.join(output_dir, filename)
            outfile = pysam.AlignmentFile(filepath, "w", template=samfile)

            if (count % size) == 0 and count != 0:
                outfile.close()
                 
                bfilename = "bwaoutput" + str(split_count) + "_split.bam"
                bfilepath = os.path.join(output_dir, filename)
               
                cmd = "samtools view -S -bh " + filepath + " > " + bfilepath
                print ("cmd: %s" %(cmd))
                completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
                print ("completedProc: %s" %(completedProc.stderr))
                channel.basic_publish(exchange='', routing_key=queue, body=filepath)
                split_count = split_count + 1
                if split_count > max_files:
                    break
                filename = "bwaoutput" + str(split_count) + "_split.bam"
                filepath = os.path.join(output_dir, filename)
                print ("Opening file: %s, %s, %s" %(str(split_count), str(count), str(filepath)))
                outfile = pysam.AlignmentFile(filepath, "w", template=samfile)

            outfile.write(read)
            count = count + 1
            print (count)
        samfile.close()
        '''        
        while (True):
            filename = "bwaoutput" + str(split_count) + "_split.bam"
            filepath = os.path.join(output_dir, filename)
            cmd = "samtools view -H" + " " + output + " > " + filepath
            completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)

            cmd = "samtools view -b" + output + " | head -n " + str(firstn) +\
                  " | tail -n " + str(size) + " >> " + filepath
            completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
            cmd = "samtools view " + output + " | head -n " + str(firstn) +\
                  " | tail -n " + str(size) + " | wc -l"
            completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
            channel.basic_publish(exchange='', routing_key=queue, body=filepath)
            print ("completedProc.stdout: %s" %(int(completedProc.stdout)))
            if (int(completedProc.stdout) < size):
                break
            firstn = firstn + size
            split_count = split_count + 1
        '''         
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
    split_count = 3
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

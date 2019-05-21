import subprocess
from subprocess import PIPE
import os
import pika
import redis

REDIS_HOST = os.environ['REDIS_HOST']
TASK_ID = os.environ['TASK_ID']
RUN_ID = os.environ['RUN_ID']
DATA_DIR = os.environ['DATA_DIR']
AMQP_SERVER = os.environ['AMQP_SERVER']
AMQP_PORT = os.environ['AMQP_PORT']
AMQP_USER = os.environ['AMQP_USER']
AMQP_PWD = os.environ['AMQP_PWD']

output_dir = os.path.join(DATA_DIR, RUN_ID, TASK_ID)

try:
    os.makedirs(output_dir)
except FileExistsError:
    pass

r = redis.Redis(host=REDIS_HOST)

out_queue = ("%s.%s" %(RUN_ID, TASK_ID))
queue_key = ("%s.queue" %(RUN_ID))
in_queue = r.get(queue_key).decode('utf=8')

total_processed = 0
completedProc = None

def run_vcfconcat(merge_list):
    global total_processed, completedProc
    inputfile_list = ""
    for name in merge_list:
        inputfile_list = inputfile_list + " " + name
        total_processed = total_processed + 1
    outputfile_name = "merge.vcf"
    outputfile = os.path.join(output_dir, outputfile_name) 
    print("Input file list is" + inputfile_list)

    cmd = "vcf-concat " + inputfile_list + " > " + outputfile
    print ("Executing: " + cmd)  
    completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
    print ("VCF Concat output: %s" %(completedProc.stdout))
    channel.basic_publish(exchange='', routing_key=out_queue, body=outputfile)


credentials = pika.PlainCredentials(AMQP_USER, AMQP_PWD)
parameters = pika.ConnectionParameters(AMQP_SERVER, AMQP_PORT, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=out_queue)
channel.basic_qos(prefetch_count=1)

merge_list = []
while True:
    print ("Waiting for task to arrive.")
    method_frame, header_frame, body = channel.basic_get(queue=in_queue, auto_ack=False)
    print (method_frame)
    if method_frame == None or method_frame.NAME == 'Basic.GetEmpty':
        print("Channel Empty.")
        # We are done, lets break the loop and stop the application.
        if merge_list:
            run_vcfconcat(merge_list)
        else:
            print ("Nothing to merge :(")
        break
    
    merge_list.append(body.decode("utf=8"))
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
channel.close()
connection.close()


#print ('VCF tools container creation completed with status code:' + ' ' + str(completedProc.returncode))

if total_processed and completedProc:
    update_key = RUN_ID + "." + TASK_ID
    r.incrby(update_key, completedProc.returncode)
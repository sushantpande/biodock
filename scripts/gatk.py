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
REFFILE = os.environ['REF_FILE']

output_dir = os.path.join(DATA_DIR, RUN_ID, TASK_ID)

try:
    os.makedirs(output_dir)
except FileExistsError:
    pass

ref = os.path.join(DATA_DIR, REFFILE)

r = redis.Redis(host=REDIS_HOST)

redis_p = r.pipeline()

out_queue = ("%s.%s" %(RUN_ID, TASK_ID))
queue_key = ("%s.queue" %(RUN_ID))
in_queue = r.get(queue_key).decode('utf=8')

total_processed = 0
completedProc = None

def rungatk(body):
    global total_processed, completedProc
    inputfile = body.decode("utf=8")
    print ("Input is: " + inputfile)
    outputfile_name = os.path.basename(inputfile) + ".vcf"
    outputfile = os.path.join(output_dir, outputfile_name) 
    print("Input file is " + inputfile)

    cmd = "gatk HaplotypeCaller -R " + ref + " -I " + inputfile + " -O " +  outputfile
    #cmd = "touch outputfile"
    print ("Executing: " + cmd)  
    completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
    print ("GATK output: %s" %(completedProc.stdout))
    channel.basic_publish(exchange='', routing_key=out_queue, body=outputfile)
    total_processed = total_processed + 1


credentials = pika.PlainCredentials(AMQP_USER, AMQP_PWD)
parameters = pika.ConnectionParameters(AMQP_SERVER, AMQP_PORT, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=out_queue)
channel.basic_qos(prefetch_count=1)

while True:
    print ("Waiting for task to arrive.")
    method_frame, header_frame, body = channel.basic_get(queue=in_queue, auto_ack=False)
    print (method_frame)
    if method_frame == None or method_frame.NAME == 'Basic.GetEmpty':
        print("Channel Empty.")
        # We are done, lets break the loop and stop the application.
        break
    
    rungatk(body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
channel.close()
connection.close()


#print ('GATK container creation completed with status code:' + ' ' + str(completedProc.returncode))

if total_processed and completedProc:
    update_key = RUN_ID + "." + TASK_ID
    r.incrby(update_key, completedProc.returncode)
    
queue_key = ("%s.queue" %(RUN_ID))
r.set(queue_key, out_queue)
print ('Queue is: %s' %(out_queue))


import subprocess
from subprocess import PIPE
import os
import sys
import utility
import conn
import yaml


process_from_queue = False

'''Get input file'''
in_queue = None
input_file = utility.get_input_file()

if not input_file:
    print ("No input file for Job, looking for input queue")

    '''Get input queue'''
    in_queue_str = utility.get_env_param('IN_QUEUE')
    in_queue_list = in_queue_str.split()

    if not in_queue_list:
        print ("Found no in_queue for job")
        sys.exit(os.EX_NOTFOUND)
    else:
        in_queue = in_queue_list[0]
        process_from_queue = True

'''Get output queue'''
out_queue = utility.get_env_param('OUT_QUEUE')

'''Get output dir'''
output_dir = utility.get_output_dir() 

'''Get AMQP connection'''
amqp_conn = conn.get_amqp_conn()

total_processed = 0
completedProc = None
output_file_list = []
input_file_list = []
channel_out = None

def runVCFConcat(input_file):
    global total_processed, completedProc, channel_out
    global output_file_list

    '''Channel to publish'''
    if not channel_out:
        channel_out = amqp_conn.channel()
        channel_out.queue_declare(queue=out_queue)

    '''inputfile expects absolute path'''
    print ("Input is (%s)" %(input_file))

    '''Set output file name with absolute path'''
    output_file_name = "merge.vcf"
    output_file = os.path.join(output_dir, output_file_name)

    '''Exec VCF command - concat'''
    cmd = "vcf-concat " + ' '.join(input_file) + " > " + output_file
    print ("Executing: " + cmd)  
    completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)

    print ("VCF output: %s" %(completedProc.stdout))
    print ("VCF error: %s" %(completedProc.stderr))

    output_file_list.append(output_file)

    '''Publish to output queue output file name with absolute path'''
    status = conn.publish_to_amqp(channel_out, routing_key=out_queue, body=output_file)
    if not status:
        print ("Cannot publish to AMQP, continuing to process, refer to meta\
                dir for manul update of AMQP")
    

if channel_out:
    conn.close_amqp_channel(channel_out)

if process_from_queue:
    '''Channel for consumption'''
    channel_in = amqp_conn.channel()
    channel_in.basic_qos(prefetch_count=1)

    '''Start consuming'''
    while True:
        print ("Waiting for task to arrive.")
        body = conn.consume_from_amqp(channel_in, in_queue)

        if not body:
            runVCFConcat(input_file_list)
            break

        input_file_list.append(body)

    '''Close consumption channel'''
    conn.close_amqp_channel(channel_in)
else:
    runVCFConcat(input_file)

'''Close AMQP connection'''
conn.close_amqp_channel(amqp_conn)

'''Update meta file'''
meta = {}
meta.update({'OUTPUT_DIR': output_dir})
meta.update({'OUT_QUEUE': out_queue})
meta.update({'IN_QUEUE': in_queue})
meta.update({'OUTPUT_FILE_LIST': output_file_list})
meta.update({'OUTPUT_FILE_COUNT': total_processed})
meta_file = utility.get_meta_file()
print ("Will write meta info. to file (%s)" %(meta_file))

with open(meta_file, 'w+') as fh:
    yaml.dump(meta, fh, default_flow_style=False)

sys.exit(os.EX_OK)

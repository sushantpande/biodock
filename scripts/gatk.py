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

'''Get Reference file'''
ref = utility.get_ref_file()

'''Get AMQP connection'''
amqp_conn = conn.get_amqp_conn()

total_processed = 0
completedProc = None
output_file_list = []
channel_out = None

def runGATK(inputfile):
    global total_processed, completedProc, channel_out
    global output_file_list

    '''Channel to publish'''
    if not channel_out:
        channel_out = amqp_conn.channel()
        channel_out.queue_declare(queue=out_queue)

    '''inputfile expects absolute path'''
    print ("Input is: " + inputfile)

    '''Set output file name with absolute path'''
    outputfile_name = os.path.basename(inputfile) + ".vcf"
    outputfile = os.path.join(output_dir, outputfile_name) 

    '''Exec GATK command - HaplotypeCaller'''
    cmd = "gatk HaplotypeCaller -R " + ref + " -I " + inputfile + " -O " +  outputfile
    print ("Executing: " + cmd)  
    completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)

    print ("GATK output: %s" %(completedProc.stdout))
    print ("GATK error: %s" %(completedProc.stderr))

    output_file_list.append(outputfile)

    '''Publish to output queue output file name with absolute path'''
    status = conn.publish_to_amqp(channel_out, routing_key=out_queue, body=outputfile)
    if not status:
        print ("Cannot publish to AMQP, continuing to process, refer to meta\
                dir for manul update of AMQP")
    
    total_processed = total_processed + 1


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
            break

        runGATK(body)
    
    '''Close consumption channel'''
    conn.close_amqp_channel(channel_in)
else:
    runGATK(input_file)

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

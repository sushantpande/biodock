import subprocess
from subprocess import PIPE
import os
import pysam
import sys
import time
import glob
import bwa_split
import conn
import utility
import yaml


redis_conn = None
process_from_queue = False

'''Get input file'''
input_file = utility.get_input_file()
in_queue = None

if not input_file:
    print ("No input file for Job, looking for input queue")

    '''Get input queue'''
    in_queue_str = utility.get_env_param('IN_QUEUE')
    in_queue_list = in_queue_str.split()
    in_queue = None

    if not in_queue_list:
        print ("Found no input queue for job")
        sys.exit(os.EX_SOFTWARE)
    else:
        in_queue = in_queue_list[0]
        process_from_queue = True


'''Get output dir'''
output_dir = utility.get_output_dir()

'''Get output file'''
output_file = os.path.join(output_dir, "bwaoutput.bam")

'''Get Reference file'''
ref = utility.get_ref_file()

'''Get AMQP connection'''
amqp_conn = conn.get_amqp_conn()

cmd = 'bwa mem -R "@RG\\tID:foo\\tLB:bar\\tPL:illumina\\tPU:illumina\\tSM:SAMPLE" ' + ref + ' ' +\
      input_file + ' | samtools sort > ' + output_file + ' && samtools index ' + output_file
#cmd = 'bwa mem -R "@RG\\tID:foo\\tLB:bar\\tPL:illumina\\tPU:illumina\\tSM:ERR000589" ' + ref + ' ' +\
#      input_file + ' | samtools sort > ' + output_file + ' && samtools index ' + output_file

print ("%s %s" %("cmd:" , cmd))

completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)
print ("BWA mem output: %s" %(completedProc.stdout))
print ("BWA mem error: %s" %(completedProc.stderr))

output_file_list = []
split = int(utility.get_env_param('BWA_SPLIT_OUTPUT'))
split_size = int(utility.get_env_param('BWA_SPLIT_OUTPUT_SIZE'))

if split == 1:
    split_output_dir = utility.get_split_dir() 

    if split_size:
        output_file_list = bwa_split.split_by_file_count(output_file, split_output_dir, split_size)
    else:    
        output_file_list = bwa_split.split_by_chr(output_file, split_output_dir)
else:
    output_file_list.append(output)

out_queue = utility.get_env_param('OUT_QUEUE')

channel_out = amqp_conn.channel()
channel_out.queue_declare(queue=out_queue)

for output_file in output_file_list:
    status = conn.publish_to_amqp(channel_out, routing_key=out_queue, body=output_file)
    if not status:
        print ("Cannot publish to AMQP, continuing to process, refer to meta\
                dir for manul update of AMQP")

'''Update meta file'''
meta = {}
meta.update({'OUTPUT_DIR': output_dir})
meta.update({'OUT_QUEUE': out_queue})
meta.update({'IN_QUEUE': in_queue})
meta.update({'OUTPUT_FILE_LIST': output_file_list})
meta.update({'OUTPUT_FILE_COUNT': len(output_file_list)})
meta_file = utility.get_meta_file()
print ("Will write meta info. to file (%s)" %(meta_file))

with open(meta_file, 'w+') as fh:
    yaml.dump(meta, fh, default_flow_style=False)

'''Update Redis server'''
redis_conn = conn.get_redis_conn()
key = ("%s.%s.%s" %(utility.run_id, utility.task_id, 'OUTPUT_FILE_COUNT'))
if redis_conn:
    status = conn.update_to_redis(redis_conn, key, len(output_file_list))
    if not status:
        print ("Failed to update redis key (%s), val (%s)" %(key, len(output_file_list)))
else:
    print ("Failed to update redis key (%s), val (%s)" %(key, len(output_file_list)))

sys.exit(os.EX_OK)

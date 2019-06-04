import os
import pika
import redis
import sys


'''Get param value from environ'''
def get_env_param(param):
    value = None
    try:
        value = os.environ[param]
    except:
        if param == 'REDIS_HOST' or param == 'AMQP_SERVER':
            value = 'localhost'
        elif param == 'AMQP_USER' or param == 'AMQP_PWD':
            value = admin
        elif value == 'AMQP_PORT':
            value = ''
        else:
            print ("Failed to get value for param (%s)" %param)
    return value

task_id = get_env_param('TASK_ID')
if not task_id:
    print ("Failed to get task ID will return with exit code (%s)" %(os.EX_SOFTWARE))
    sys.exit(os.EX_SOFTWARE)

run_id = get_env_param('RUN_ID')
if not run_id:
    print ("Failed to get run ID will return with exit code (%s)" %(os.EX_SOFTWARE))
    sys.exit(os.EX_SOFTWARE)

data_dir = get_env_param('DATA_DIR')
if not data_dir:
    print ("Failed to get data dir will return with exit code (%s)" %(os.EX_SOFTWARE))
    sys.exit(os.EX_SOFTWARE)

work_dir = get_env_param('WORK_DIR')
if not work_dir:
    print ("Failed to get work dir will return with exit code (%s)" %(os.EX_SOFTWARE))
    sys.exit(os.EX_SOFTWARE)



'''Get the handle to the queue this job will write into'''
def get_output_queue_name():
    return ("%s.%s" %(run_id, task_id))


'''Get the handle to the queue this job will read from'''
def get_input_queue_name():
    return (get_from_redis(redis_conn, key))


def get_output_dir():
    dir = os.path.join(data_dir, run_id, task_id)

    try:
        os.makedirs(dir)
    except FileExistsError:
        pass

    return (dir)


def get_ref_file():
    file = get_env_param('REF_FILE')
    if not file:
        print ("Failed to get ref. file")
        return file
    return (os.path.join(data_dir, file))


def get_meta_file():
    file = ".meta"
    dir = os.path.join(work_dir, run_id, task_id)

    try:
        os.makedirs(dir)
    except FileExistsError:
        pass
    
    return (os.path.join(dir, file))


def get_split_dir():
    file = "split"
    dir = os.path.join(data_dir, run_id, task_id, "split")

    try:
        os.makedirs(dir)
    except FileExistsError:
        pass
    
    return dir


def get_input_file():
    param = ("%s_%s" %(task_id, 'INPUT'))
    file = get_env_param(param)
    if not file:
        print ("Failed to get input file")
        return file
    return os.path.join(data_dir, file)

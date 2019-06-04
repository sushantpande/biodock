import hashlib
import string
import random
import time
import logging
from pprint import pprint
import yaml
import sys,os, time
import kubernetes
from kubernetes import client


k8_host = "https://localhost:40097"
kubernetes.config.load_kube_config()
configuration = client.Configuration()
configuration.host = k8_host
configuration.watch = True
api_instance = client.BatchV1Api(client.ApiClient(configuration))


def kube_create_job_object(name, container_image, container_name, namespace, env_vars, parallelism, cmd, image_pull_policy='Always'):
    '''
    Create a k8 Job Object
    '''
    ''' Body is the object Body '''
    body = client.V1Job(api_version="batch/v1", kind="Job")
    ''' Configure Metadata '''
    body.metadata = client.V1ObjectMeta(namespace=namespace, name=name)
    ''' And a Status '''
    body.status = client.V1JobStatus()
    ''' Configure Template '''
    template = client.V1PodTemplate()
    template.template = client.V1PodTemplateSpec()
    ''' Passing Arguments in Env: '''
    env_list = []
    for env_name, env_value in env_vars.items():
        env_list.append( client.V1EnvVar(name=env_name, value=env_value) )
    ''' Configure command and arguments '''
    command = ["/bin/bash"]
    args = ["-c"]
    mnt_mk_dir = "mkdir -p /mnt/efs"
    mnt_cmd = "mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=300,retrans=1,noresvport 172.31.42.102:/ /mnt/efs"
    arg_string = ("%s && %s && %s" % (mnt_mk_dir, mnt_cmd, cmd))
    args.append(arg_string)
    print (len(args))
    print (args)
    ''' Configure security context and capabilities '''
    capabilities = client.V1Capabilities(add=["SYS_ADMIN"])
    security_context = client.V1SecurityContext(capabilities=capabilities)
    container = client.V1Container(name=container_name, image=container_image, env=env_list, command=command, args=args, security_context=security_context, image_pull_policy=image_pull_policy)
    template.template.spec = client.V1PodSpec(containers=[container], restart_policy='Never')
    ''' Configure Spec '''
    body.spec = client.V1JobSpec(parallelism=parallelism, ttl_seconds_after_finished=600, template=template.template)
    return body


def kube_run_job_object(namespace, job):
    try: 
       api_response = api_instance.create_namespaced_job("default", job, pretty=True)
       print(api_response)
    except kubernetes.client.rest.ApiException as e:
       print("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)
    return


def kube_monitor_job_status(namespace, name):
    STATUS_COND_TYPE_COMPLETE = "Complete"
    STATUS_COND_STATUS_TRUE = "True"
    INTERESTED_EVENTS = ["MODIFIED", "DELETED"]
    api_response = None
    ''' Watch begins! '''
    w = kubernetes.watch.Watch()
    stream = w.stream(api_instance.list_namespaced_job, namespace)
    for event in stream:
        if event['object'].metadata.name == name:
            if event['type'] in INTERESTED_EVENTS:
                status = event['object'].status
                if not (status.failed):
                    if not (status.active):
                        conditions  = status.conditions[0]
                        if conditions.type == STATUS_COND_TYPE_COMPLETE and STATUS_COND_STATUS_TRUE == "True":
                            print ("Job (%s) status is (%s)" %(name, conditions.type))
                            #Call clean up here
                            break
                    else:
                        print ("Job (%s) has (%s) active nodes" %(name, status.active))
                else:        
                    print ("Job (%s) has (%s) failed nodes" %(name, status.failed))
    return True


def kube_delete_complete_jobs_pod(jobname, namespace):
    deleteoptions = client.V1DeleteOptions()
    api_pods = client.CoreV1Api()
    try:
        pods = api_pods.list_namespaced_pod(namespace,
                                            include_uninitialized=False,
                                            pretty=True,
                                            timeout_seconds=60)
    except kubernetes.client.rest.ApiException as e:
        print("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)
        return False
    
    for pod in pods.items:
        print (pod)
        pod_job_name = pod.metadata.labels.get('job-name', '')
        if pod_job_name == jobname:
            podname = pod.metadata.name
            try:
                api_response = api_pods.delete_namespaced_pod(podname, namespace, body=deleteoptions)
                print (api_response)
            except kubernetes.client.rest.ApiException as e:
                print ("Exception when calling CoreV1Api->delete_namespaced_pod: %s\n" % e)

    return True


def kube_cleanup_complete_jobs(jobname, namespace):
    deleteoptions = client.V1DeleteOptions()
    try: 
        jobs = api_instance.list_namespaced_job(namespace,
                                                include_uninitialized=False,
                                                pretty=True,
                                                timeout_seconds=60)
    except kubernetes.client.rest.ApiException as e:
        print("Exception when calling BatchV1Api->list_namespaced_job: %s\n" % e)
        return False
    for job in jobs.items:
        print(job)
        if jobname == job.metadata.name:
            jobname = job.metadata.name
            jobstatus = job.status.conditions
            parallelism = job.spec.parallelism
            if jobstatus and job.status.succeeded == parallelism:
                '''Clean up Job'''
                print("Cleaning up Job: {}. Completed at: {}".format(jobname, job.status.completion_time))
                try: 
                    api_response = api_instance.delete_namespaced_job(jobname, namespace, body=deleteoptions)
                    print(api_response)
                    '''API to delete pods''' 
                    kube_delete_complete_jobs_pod(jobname, namespace)
                except kubernetes.client.rest.ApiException as e:
                    print("Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)
            else:
                active = job.status.active
                failed = job.status.failed
                if jobstatus is None and (active > 0 or failed > 0):
                    print("Job: {} not cleaned up. Current status: active-{} failed-{}".format(jobname, active, failed))
    
    return True

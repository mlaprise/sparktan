"""Run your script on a Spark EMR cluster.

Usage:
    sparktan run [<jobflow_id>] [options]
    sparktan quickstart <project>
    sparktan terminate <jobflow_id>
    sparktan update-venv <jobflow_id>
    sparktan list

Options:
    --job-args=<ja>         The args to pass to the main job
    --terminate-cluster     Kill the cluster after running the script
    -h --help               Show this help screen
"""
from __future__ import absolute_import

import os
import importlib
import json
import logging
import time
import uuid
import pprint
import subprocess

import boto3
import fabric.api as fab

from fabric.api import run, env, put, local
from fabric.tasks import execute
from docopt import docopt
from sparktan import bootstrap


log = logging.getLogger('spark_cluster')


def run_spark_script(script, keyfile, host, spark_config, venv_name, job_args):
    job_uuid =  str(uuid.uuid4())
    def _run_spark_script():
        run('mkdir -p /home/hadoop/sparktan')
        run('mkdir /home/hadoop/sparktan/{}'.format(job_uuid))
        put(script, '/home/hadoop/sparktan/{}/main.py'.format(job_uuid))
        command = ("PYSPARK_PYTHON=/home/hadoop/virtualenvs/%(venv_name)s/bin/python "
                   "/usr/lib/spark/bin/spark-submit  "
                   "--master=yarn-client "
                   "--num-executors=%(num_executors)s "
                   "--executor-cores=%(executor_cores)s "
                   "--executor-memory=%(executor_memory)s "
                   "/home/hadoop/sparktan/%(job_uuid)s/main.py "
                   "%(script_args)s" %
                   {'venv_name': venv_name,
                    'job_uuid': job_uuid,
                    'num_executors': spark_config['num_executors'],
                    'executor_cores': spark_config['executor_cores'],
                    'executor_memory': spark_config['executor_memory'],
                    'script_args': job_args or ''}
                   )
        run(command)
    return _run_spark_script


def update_venv(here, jobflow_id, venv_name, key_filename):
    log.info('Updating virtual environment...')
    fabfile_path = os.path.split(here)[0] + '/envs'
    fab_command = 'fab cluster:{} venv:{} key:{} create_venv --fabfile={}/fabfile.py'.format(jobflow_id, venv_name, key_filename, fabfile_path)
    local(fab_command)


def list_existing_cluster(client, project_name):
    res = client.list_clusters(ClusterStates=['STARTING','BOOTSTRAPPING','RUNNING','WAITING',])
    project_clusters = [cluster for cluster in res['Clusters'] if cluster['Name'] == project_name]
    # Fetch the master
    more_info = client.describe_job_flows(JobFlowIds=[c['Id'] for c in project_clusters])
    host_per_cluster = {c['JobFlowId']:c['Instances']['MasterPublicDnsName'] for c in more_info['JobFlows']}

    for cluster in project_clusters:
        cluster['MasterPublicDnsName'] = host_per_cluster[cluster['Id']]
        # Fetch the master private ip
        instances_info = client.list_instances(ClusterId=cluster['Id'])
        private_ips = {inst['PublicDnsName']:inst['PrivateIpAddress'] for inst in instances_info['Instances']}
        cluster['MasterPrivateIpAddress'] = private_ips[cluster['MasterPublicDnsName']]
    return project_clusters


def main():
    args = docopt(__doc__)
    # Logging
    root_logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    here = os.path.split(os.path.abspath(__file__))[0]

    if args['quickstart']:
        bootstrap.quickstart(args['<project>'])
        return

    cluster_config = json.loads(open('./config.json', 'r').read())
    env.key_filename = cluster_config.pop('KeyFile')
    spark_config = cluster_config.pop('SparkConfig')

    if args['update-venv']:
        update_venv(here, args['<jobflow_id>'], cluster_config['Name'], cluster_config['KeyFile'])

    client = boto3.client('emr')

    if args['terminate']:
        response = client.terminate_job_flows(JobFlowIds=[args['<jobflow_id>']])
        log.info('Terminating Cluster...')
        log.info(response)

    if args['list']:
        # list the existing cluster with this project name
        clusters = list_existing_cluster(client, cluster_config['Name'])
        for cluster_data in clusters:
            pprint.pprint(cluster_data)

    if not args['run']:
        return

    # Start a cluster if neened
    if not args['<jobflow_id>']:
        emr_response = client.run_job_flow(**cluster_config),

        if emr_response[0]['ResponseMetadata']['HTTPStatusCode'] == 200:
            jobflow_id = emr_response[0]['JobFlowId']
            log.info('Created new jobflow: {}'.format(jobflow_id))
        else:
            log.info('Cluster creation failed: {}'.format(emr_response))

        # Make sure the cluster is ready
        log.info('Bootstrapping the cluster')
        waiter = client.get_waiter('cluster_running')
        waiter.wait(ClusterId=jobflow_id)
        log.info('Cluster {} is now running'.format(jobflow_id))

        # Create the venv for the first time
        update_venv(here, jobflow_id, cluster_config['Name'])

    else:
        jobflow_id = args['<jobflow_id>']

    # Run the script on the cluster
    cluster_info = client.describe_cluster(ClusterId=jobflow_id)
    master_host = cluster_info['Cluster']['MasterPublicDnsName']

    script = "./{}".format('main.py')
    output = execute(run_spark_script(script,
                                      env.key_filename,
                                      master_host,
                                      spark_config,
                                      cluster_config['Name'],
                                      args['--job-args']), hosts=["hadoop@{}".format(master_host)])
    for line in output:
        log.info(line)

    if args['--terminate-cluster']:
        response = client.terminate_job_flows(JobFlowIds=[jobflow_id])
        log.info('Terminating Cluster...')
        log.info(response)
    else:
        log.info('''Cluster {} is still running: use "sparktan terminate YOUR_CLUSTER_ID" to terminate it'''.format(jobflow_id))


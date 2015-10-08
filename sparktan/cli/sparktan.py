"""Run your script on a Spark EMR cluster.

Usage:
    sparktan run [<project>] [options]
    sparktan quickstart <project> [options]
    sparktan terminate <jobflow_id>
    sparktan update-venv <jobflow_id>

Options:
    --cluster-name=<cn>          Cluster Name
    --jobflow-id=<jf>            Update an existing jobflow
    --terminate-cluster          Kill the cluster after running the script [default: True]
    -h --help                    Show this help screen
"""
from __future__ import absolute_import

import os
import importlib
import json
import logging
import time
import uuid
import subprocess

import boto3
import fabric.api as fab

from fabric.api import run, env, put, local
from fabric.tasks import execute
from docopt import docopt
from sparktan import bootstrap


log = logging.getLogger('spark_cluster')


def run_spark_script(script, keyfile, host, spark_config, venv_name):
    job_uuid =  str(uuid.uuid4())
    def _run_spark_script():
        run('mkdir -p /home/hadoop/sparktan')
        run('mkdir /home/hadoop/sparktan/{}'.format(job_uuid))
        put(script, '/home/hadoop/sparktan/{}/main.py'.format(job_uuid))
        command = ("PYSPARK_PYTHON=/home/hadoop/miniconda/envs/%(venv_name)s/bin/python "
                   "/usr/lib/spark/bin/spark-submit  "
                   "--master=yarn-client "
                   "--num-executors=%(num_executors)s "
                   "--executor-cores=%(executor_cores)s "
                   "--executor-memory=%(executor_memory)s "
                   "/home/hadoop/sparktan/%(job_uuid)s/main.py" %
                   {'venv_name': venv_name,
                    'job_uuid': job_uuid,
                    'num_executors': spark_config['num_executors'],
                    'executor_cores': spark_config['executor_cores'],
                    'executor_memory': spark_config['executor_memory']}
                   )
        run(command)
    return _run_spark_script


def update_venv(here, jobflow_id, venv_name):
    log.info('Updating virtual environment...')
    fabfile_path = os.path.split(here)[0] + '/envs'
    fab_command = 'fab cluster:{} venv:{} create_venv --fabfile={}/fabfile.py'.format(jobflow_id, venv_name, fabfile_path)
    local(fab_command)


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

    if args['<project>']:
        project_path = args['<project>']
    else:
        project_path = '.'

    if args['quickstart']:
        bootstrap.quickstart(args['<project>'])

    cluster_config = json.loads(open('{}/config.json'.format(project_path), 'r').read())
    env.key_filename = cluster_config.pop('KeyFile')
    spark_config = cluster_config.pop('SparkConfig')

    if args['update-venv']:
        update_venv(here, args['<jobflow_id>'], cluster_config['Name'])

    client = boto3.client('emr')

    if args['terminate']:
        response = client.terminate_job_flows(JobFlowIds=[args['<jobflow_id>']])
        log.info('Terminating Cluster...')
        log.info(response)

    if not args['run']:
        return

    # Start a cluster if neened
    if not args['--jobflow-id']:
        # Override the cluster name
        if args['--cluster-name']:
            cluster_config['Name'] =  args['--cluster-name']

        emr_response = client.run_job_flow(**cluster_config),

        if emr_response[0]['ResponseMetadata']['HTTPStatusCode'] == 200:
            jobflow_id = emr_response[0]['JobFlowId']
            log.info('Created new jobflow: {}'.format(jobflow_id))
        else:
            log.info('Cluster creation failed: {}'.format(emr_response))

        # Create the venv for the first time
        update_venv(here, jobflow_id, cluster_config['Name'])

    else:
        jobflow_id = args['--jobflow-id']

    # Make sure the cluster is ready
    log.info('Bootstrapping the cluster')
    waiter = client.get_waiter('cluster_running')
    waiter.wait(ClusterId=jobflow_id)
    log.info('Cluster {} is now running'.format(jobflow_id))


    # Run the script on the cluster
    cluster_info = client.describe_cluster(ClusterId=jobflow_id)
    master_host = cluster_info['Cluster']['MasterPublicDnsName']

    script = "{}/{}".format(project_path, 'main.py')
    output = execute(run_spark_script(script, env.key_filename, master_host, spark_config, cluster_config['Name']), hosts=["hadoop@{}".format(master_host)])
    for line in output:
        log.info(line)

    # Kill the cluster
    if args['--terminate-cluster']:
        response = client.terminate_job_flows(JobFlowIds=[jobflow_id])
        log.info('Terminating Cluster...')
        log.info(response)
    else:
        log.info('''Cluster {} is still running: use "sparktan terminate" to terminate it'''.format(jobflow_id))


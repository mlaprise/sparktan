import os
import boto3

from glob import glob
import fabric.api as fab
from fabric.decorators import task, hosts
from fabric.api import env
from fabric.state import output
from fabric.contrib.files import exists
from fabric.api import (cd, env, lcd, local, parallel, put, puts, run,
                        runs_once, settings, show, sudo)

env.forward_agent = True
env.output_prefix = False
env.key_filename = "/Users/mlaprise/.ssh/emr_jobs.pem"
env.user = 'hadoop'
env.disable_known_hosts = True

output['stdout'] = True
output['stderr'] = True

venv_root_path = '/home/hadoop/miniconda/envs'
conda_requirements_path = 'conda_requirements.txt'
pip_requirements_path = 'pip_requirements.txt'


def run_in_virtualenv(command, venv):
    source = 'source activate %s && ' % venv
    fab.run(source + command)


emr_client = boto3.client('emr')


def get_nodes(cluster_id):
    puts("Fetching nodes...")
    try:
        res = emr_client.list_instances(ClusterId=cluster_id)
    except:
        return []
    nodes_host = [instance['PublicDnsName'] for instance in res['Instances']]
    return nodes_host


def update_virtualenv(repo_name,
                      virtualenv_path,
                      build_path,
                      conda_requirements_file='conda_requirements.txt',
                      pip_requirements_file='pip_requirements.txt'):
    """Update the virtualenv using a requirements file.
    :param repo_name:         Name of the repo being updated. Used to contruct path to project.
    :param virtualenv_path:   Path to the root of the remote virutalenv
    :param build_path:        Path to the root of the build
    :param requirements_file: Name of requirements file(s) to use, may be a string or a list.
    """
    if exists(virtualenv_path) is False:
        puts("Virtualenv not found - creating one for %s" % repo_name)
        with settings(warn_only=True):
            run("conda create --name {} python=2.7 pip --yes".format(env.venv_name))

    puts("Updating the virtualenv")
    here = os.getcwd()

    # copythe deployment key and script to the remote machine
    package_path = os.path.split(os.path.abspath(__file__))[0]
    deployment_key = env.key_filename
    sudo("rm -f %s" % os.path.join(build_path, deployment_key))
    put(os.path.join(here, deployment_key),
        build_path + '/', mode=0o600)
    put(os.path.join(package_path, 'git_ssh.sh'),
        build_path + '/', mode=0o775)
    git_ssh = os.path.join(build_path, 'git_ssh.sh')

    virtualenv = os.path.split(virtualenv_path)[-1]
    # Run the conda update
    with settings(show('warnings', 'running', 'stdout', 'stderr')):
        run('CONDA_DEFAULT_ENV="%s" CONDA_ENV_PATH="%s" conda install --file %s --yes' % (virtualenv, virtualenv_path, os.path.join(build_path, conda_requirements_file)))

    # Run the pip update
    pip = os.path.join(virtualenv_path, 'bin/pip')
    pip_options = [
        '--allow-all-external'
    ]
    with settings(show('warnings', 'running', 'stdout', 'stderr')):
        run('GIT_SSH=%s %s install %s -r %s' % (git_ssh,
                                                 pip,
                                                 " ".join(pip_options),
                                                 os.path.join(build_path, pip_requirements_file)))

    sudo('chmod -R g+ws %s' % virtualenv_path)


@task
def cluster(cluster_id):
    """Set hosts dynamically using the cluster info."""
    env.cluster_id = cluster_id
    env.hosts = get_nodes(cluster_id)


@task
def venv(venv_name):
    env.venv_name = venv_name


@task
@parallel
def create_venv():
    venv_path = os.path.join(venv_root_path, env.venv_name)
    tmpdir = fab.run('mktemp -d')
    here = os.getcwd()

    try:
        # Create and/or update venv on node.
        with fab.lcd(here):
            fab.put(conda_requirements_path, tmpdir)
            fab.put(pip_requirements_path, tmpdir)
        conda_req_filename = os.path.basename(conda_requirements_path)
        pip_req_filename = os.path.basename(pip_requirements_path)
        update_virtualenv(env.venv_name, venv_path, tmpdir, conda_req_filename, pip_req_filename)

    finally:
        fab.run('rm -rf %s' % tmpdir)

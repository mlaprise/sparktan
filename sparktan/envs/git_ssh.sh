# git_ssh.sh -- use a local ssh key for a git clone
#
# This is how we can do deployments on machines which don't have a github
# key installed. Otherwise, we'd need to generate and add a key for every
# machine we add.
#
# Original Idea: http://stackoverflow.com/questions/4565700/specify-private-ssh-key-to-use-when-executing-shell-command-with-or-without-ruby/7929041#7929041
#
# To run: GIT_SSH=<this script> pip install -r requirements.txt

ssh -o StrictHostKeyChecking=no -i `dirname $0`/emr_jobs.pem $1 $2

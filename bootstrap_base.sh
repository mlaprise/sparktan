#
# Install some basic dev tools we might need.
#
sudo yum update -y
sudo yum install make automake gcc gcc-c++ kernel-devel git-core -y
sudo yum install htop -y

#
# Bootstrap venvs
#
sudo pip-2.7 install virtualenv
sudo pip-2.7 install virtualenvwrapper
mkdir /home/hadoop/virtualenvs

#
# Install some basic dev tools we might need.
#
sudo yum update -y
sudo yum install make automake gcc gcc-c++ kernel-devel git-core -y
sudo yum install htop -y

#
# Bootstrap venvs
#
wget http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b
echo -e "\nexport PATH=~/miniconda/bin:$PATH" >> ~/.bashrc
source ~/.bashrc
conda create --name sparktan python=2.7 --yes

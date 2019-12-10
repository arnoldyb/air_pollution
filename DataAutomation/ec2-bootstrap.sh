#!/bin/bash
sudo yum update -y > /tmp/bootstrap.log

# Install Anaconda
wget https://repo.anaconda.com/archive/Anaconda3-2019.10-Linux-x86_64.sh >> /tmp/bootstrap.log
chmod 775 Anaconda3-2019.10-Linux-x86_64.sh
./Anaconda3-2019.10-Linux-x86_64.sh >> /tmp/bootstrap.log

# Set Path
sudo echo "export PATH=~/anaconda3/bin:$PATH" >> ~/.bash_profile

# Install tmux
sudo yum install tmux -y

# Install Libraries
conda create -n mids python=3.7
conda activate mids
pip install awscli --upgrade >> /tmp/bootstrap.log
conda install -c anaconda pandas >> /tmp/bootstrap.log
conda install -c conda-forge boto3 >> /tmp/bootstrap.log
conda install -c conda-forge s3fs >> /tmp/bootstrap.log
conda install -c conda-forge fastparquet >> /tmp/bootstrap.log
pip install flask >> /tmp/bootstrap.log
pip install Flask-JSGlue >> /tmp/bootstrap.log

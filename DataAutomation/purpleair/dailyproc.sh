#!/bin/bash

# This script is scheduled using cron on an ec2 instance to run at 2 AM every day
# It combines the 5 minute purple air data files for each day into a single file,
# adds the relevant transformations and uploads the file to s3

# Set Path
PATH="/home/ec2-user/.local/bin:/home/ec2-user/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH"

# Run Python script
cd ~/bin
conda activate mids
python dailyproc.py

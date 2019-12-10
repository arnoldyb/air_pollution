#!/bin/bash

# This script is scheduled using cron on an ec2 instance to run at 11 AM every day
# It downloads the tar file with the current month data, untars it
# and uploads the relevant data files to s3

# Set Path
PATH="/home/ec2-user/.local/bin:/home/ec2-user/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:$PATH"

# Set date variable in local time zone
stnsuffix=`TZ=":America/Los_Angeles" date +'%Y%m'`

# Download file and untar
wget ftp://ftp.ncdc.noaa.gov/pub/download/hidden/onemin/fmd_$stnsuffix.tar.Z -P /home/ec2-user/bin/
tar -xvzf /home/ec2-user/bin/fmd_$stnsuffix.tar.Z --directory /home/ec2-user/bin/datafiles

# Activate VM
source /home/ec2-user/python3/bin/activate

# Station Metadata
stnprefix='64010'
stations=('KAPC' 'KBLU' 'KCCR' 'KHWD' 'KLVK' 'KMAE' 'KMCE' 'KMOD' 'KMRY' 'KMYV' 'KNUQ' 'KOAK' 'KOVE' 'KPRB' 'KSAC' 'KSBP' 'KSCK' 'KSFO' 'KSJC' 'KSMF' 'KSNS' 'KSTS' 'KUKI' 'KVCB' 'KWVI')

# Loop through station list and upload files to s3
for i in "${stations[@]}"
do
        aws s3 cp /home/ec2-user/bin/datafiles/$stnprefix$i$stnsuffix s3://midscapstone-whos-polluting-my-air/AsosRaw/$stnprefix$i$stnsuffix
        aws s3api put-object-acl --bucket midscapstone-whos-polluting-my-air --key AsosRaw/$stnprefix$i$stnsuffix --acl public-read
        echo $stnprefix$i$stnsuffix >> /home/ec2-user/bin/ec.txt
done

# Cleanup
rm /home/ec2-user/bin/fmd_$stnsuffix.tar.Z
rm /home/ec2-user/bin/datafiles/*

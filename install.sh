#!/bin/bash

## Add Docker repository
sudo apt-get install -y curl apt-transport-https ca-certificates software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

## Install libraries and packages
sudo apt-get update -y
sudo apt-get install -y docker-ce openssl sqlite3 python3.5 python3-pip
pip3 install docker psutil

## Create EFS directory and copy file
mkdir -p /root/EFS/certs
cp EFS.py Monitor.py Scheduler.py config.ini /root/EFS/

docker build Docker/ -t arek/alpine_ssh
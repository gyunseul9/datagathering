#!/bin/bash

TIME=`date +"%Y-%m-%d_%H:%M:%S"`

DIR="/home/ubuntu/datagathering/"

source ${DIR}bin/activate

OUTPUT=$(python3 ${DIR}gathering.py)
echo -e "\n"$OUTPUT"\n"


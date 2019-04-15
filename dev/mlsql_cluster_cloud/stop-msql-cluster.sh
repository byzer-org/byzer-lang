#!/usr/bin/env bash

set -e
set -o pipefail

for env in AK AKS MLSQL_KEY_PARE_NAME; do
  if [[ -z "${!env}" ]]; then
    echo "===$env must be set to run this script==="
    exit 1
  fi
done

count=0

pids=""


while read LINE
do
    if [[ -z $LINE ]];then
     echo "$LINE is empty skip."
   else
     pymlsql stop --instance-id $LINE &
     pid=$!
     pids[${count}]=${pid}
     let "count+=1"
   fi
done <<< "$(cat mlsql.master mlsql.slaves)"

FAIL_NUM=0

for job in ${pids[*]};
do
    echo $job
    wait $job || let "FAIL_NUM+=1"
done

echo "FAILs: ${FAIL_NUM}"
echo ${pids}

if [[ ${FAIL_NUM} == "0" ]];then
    rm mlsql.master
    rm mlsql.slaves
    rm cluster.info
fi

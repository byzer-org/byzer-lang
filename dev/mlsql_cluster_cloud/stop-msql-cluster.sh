#!/usr/bin/env bash

for env in AK AKS MLSQL_KEY_PARE_NAME; do
  if [[ -z "${!env}" ]]; then
    echo "===$env must be set to run this script==="
    exit 1
  fi
done

cat mlsql.master mlsql.slaves | while read LINE; do
   if [[ -z $LINE ]];then
     echo "$LINE is empty skip."
   else
     pymlsql stop --instance-id $LINE
   fi
done
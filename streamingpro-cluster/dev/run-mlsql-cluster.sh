#!/usr/bin/env bash

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/docker-command.sh"

docker run --name mlsql-cluster \
-d --network mlsql-network \
-p 8080:8080 \
techmlsql/mlsql-cluster:1.1.7-SNAPSHOT
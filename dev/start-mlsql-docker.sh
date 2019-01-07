#!/usr/bin/env bash

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/docker-command.sh"
cd $SELF
#set -e
#set -o pipefail

DOCKER_IMAGE=${DOCKER_IMAGE:-techmlsql/mlsql:spark_2.3-1.1.7-SNAPSHOT}

docker run --name mlsql-server -d -p 9003:9003 ${DOCKER_IMAGE}

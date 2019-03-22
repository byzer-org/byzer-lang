#!/usr/bin/env bash

java -cp .:${MLSQL_CLUSTER_JAR} tech.mlsql.cluster.ProxyApplication \
-config ${MLSQL_CLUSTER_CONFIG_FILE}
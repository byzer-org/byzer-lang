#!/usr/bin/env bash
SELF=$(cd $(dirname $0) && pwd)
cd $SELF
cd ../..

export MLSQL_CLUSTER_VERSION=${MLSQL_CLUSTER_VERSION:-1.2.0-SNAPSHOT}

mvn -DskipTests -Pcluster-shade -am -pl streamingpro-cluster clean package
cd $SELF
cd ..
cp target/streamingpro-cluster-${MLSQL_CLUSTER_VERSION}.jar ./dev/mlsql-cluster-docker

cd $SELF
docker build --build-arg MLSQL_CLUSTER_JAR=streamingpro-cluster-${MLSQL_CLUSTER_VERSION}.jar -t mlsql-cluster:${MLSQL_CLUSTER_VERSION} ./mlsql-cluster-docker
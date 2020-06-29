#!/usr/bin/env bash
SELF=$(cd $(dirname $0) && pwd)
cd $SELF
cd ../..

export MLSQL_CLUSTER_VERSION=${MLSQL_CLUSTER_VERSION:-1.2.0-SNAPSHOT}

mvn -DskipTests -Pcluster-shade -am -pl streamingpro-cluster clean package
cd $SELF
cd ..
## streamingpro-cluster-2.4_2.11-1.6.0.jar
export JAR_NAME=streamingpro-cluster-2.4_2.11-${MLSQL_CLUSTER_VERSION}.jar
cp target/${JAR_NAME} ./dev/mlsql-cluster-docker

cd $SELF
docker build --build-arg MLSQL_CLUSTER_JAR=${JAR_NAME} -t mlsql-cluster:${MLSQL_CLUSTER_VERSION} ./mlsql-cluster-docker
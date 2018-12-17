#!/usr/bin/env bash

function exit_with_usage {
  cat << EOF
usage: package
run package command based on different spark version.
Inputs are specified with the following environment variables:

MLSQL_SPARK_VERSIOIN - the spark version, 2.2/2.3/2.4
DRY_RUN true|false
EOF
  exit 1
}

set -e
set -o pipefail

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

for env in MLSQL_SPARK_VERSIOIN DRY_RUN; do
  if [[ -z "${!env}" ]]; then
    echo "===$env must be set to run this script==="
    echo "===Please run ./dev/package.sh help to get how to use.==="
    exit 1
  fi
done

rm -rf /tmp/temp_ServiceFramework
git clone --depth 1 https://github.com/allwefantasy/ServiceFramework.git /tmp/temp_ServiceFramework
cd /tmp/temp_ServiceFramework
mvn install -DskipTests -Pjetty-9 -Pweb-include-jetty-9

cd -

BASE_PROFILES="-Pscala-2.11 -Ponline -Phive-thrift-server -Pcarbondata  -Pcrawler"

if [[ "$MLSQL_SPARK_VERSIOIN" > "2.2" ]]; then
  BASE_PROFILES="$BASE_PROFILES -Pdsl -Pxgboost"
else
  BASE_PROFILES="$BASE_PROFILES -Pdsl-legacy"
fi

BASE_PROFILES="$BASE_PROFILES -Pspark-$MLSQL_SPARK_VERSIOIN.0 -Pstreamingpro-spark-$MLSQL_SPARK_VERSIOIN.0-adaptor"

export MAVEN_OPTS="-Xmx6000m"

if [[ ${DRY_RUN} == "true" ]];then

cat << EOF
mvn clean package \
-Pshade \
-DskipTests \
-pl streamingpro-mlsql \
-am ${BASE_PROFILES}
EOF

else
mvn clean package -Pshade -DskipTests -pl streamingpro-mlsql -am ${BASE_PROFILES}
fi




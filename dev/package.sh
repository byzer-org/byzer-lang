#!/usr/bin/env bash

function exit_with_usage {
  cat << EOF
usage: package
run package command based on different spark version.
Inputs are specified with the following environment variables:

MLSQL_SPARK_VERSION - the spark version, 2.2/2.3/2.4
DRY_RUN true|false
DISTRIBUTION true|false
EOF
  exit 1
}

set -e
set -o pipefail

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

SELF=$(cd $(dirname $0) && pwd)
cd $SELF

cd ..

MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION:-2.3}
DRY_RUN=${DRY_RUN:-false}
DISTRIBUTION=${DISTRIBUTION:-false}
OSS_ENABLE=${OSS_ENABLE:-false}
COMMAND=${COMMAND:-package}

for env in MLSQL_SPARK_VERSION DRY_RUN DISTRIBUTION; do
  if [[ -z "${!env}" ]]; then
    echo "===$env must be set to run this script==="
    echo "===Please run ./dev/package.sh help to get how to use.==="
    exit 1
  fi
done

BASE_PROFILES="-Pscala-2.11 -Ponline -Phive-thrift-server -Pcarbondata  -Pcrawler"

if [[ "$MLSQL_SPARK_VERSION" > "2.2" ]]; then
  BASE_PROFILES="$BASE_PROFILES -Pdsl -Pxgboost"
else
  BASE_PROFILES="$BASE_PROFILES -Pdsl-legacy"
fi

BASE_PROFILES="$BASE_PROFILES -Pspark-$MLSQL_SPARK_VERSION.0 -Pstreamingpro-spark-$MLSQL_SPARK_VERSION.0-adaptor"

if [[ ${DISTRIBUTION} == "true" ]];then
BASE_PROFILES="$BASE_PROFILES -Passembly"
else
BASE_PROFILES="$BASE_PROFILES -pl streamingpro-mlsql -am"
fi

export MAVEN_OPTS="-Xmx6000m"

SKIPTEST=""
TESTPROFILE=""

if [[ "${COMMAND}" == "package" ]];then
  BASE_PROFILES="$BASE_PROFILES -Pshade"
fi

if [[ "${COMMAND}" == "package" || "${COMMAND}" == "deploy" ]];then
   SKIPTEST="-DskipTests"
fi


if [[ "${COMMAND}" == "test" ]];then
   TESTPROFILE="-Punit-test"
fi

if [[ "${COMMAND}" == "deploy" ]];then
   BASE_PROFILES="$BASE_PROFILES -Prelease-sign-artifacts"
   BASE_PROFILES="$BASE_PROFILES -Pdisable-java8-doclint"
fi

if [[ "${OSS_ENABLE}" == "true" ]];then
   BASE_PROFILES="$BASE_PROFILES -Poss-support"
fi

if [[ ${DRY_RUN} == "true" ]];then

cat << EOF
mvn clean ${COMMAND}  ${SKIPTEST} ${BASE_PROFILES}  ${TESTPROFILE}
EOF

else
cat << EOF
mvn clean ${COMMAND}  ${SKIPTEST} ${BASE_PROFILES}  ${TESTPROFILE}
EOF
mvn clean ${COMMAND}  ${SKIPTEST} ${BASE_PROFILES} ${TESTPROFILE}
fi




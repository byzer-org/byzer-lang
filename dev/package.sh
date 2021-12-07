#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

function exit_with_usage {
  cat << EOF
usage: package
run package command based on different spark version.
Inputs are specified with the following environment variables:

MLSQL_SPARK_VERSION - the spark version, 2.4/3.0 default 2.4
DRY_RUN true|false               default false
DISTRIBUTION true|false          default false
DATASOURCE_INCLUDED true|false   default false
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

MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION:-2.4}
DRY_RUN=${DRY_RUN:-false}
DISTRIBUTION=${DISTRIBUTION:-false}
OSS_ENABLE=${OSS_ENABLE:-false}
DATASOURCE_INCLUDED=${DATASOURCE_INCLUDED:-false}
COMMAND=${COMMAND:-package}

ENABLE_JYTHON=${ENABLE_JYTHON=-true}
ENABLE_CHINESE_ANALYZER=${ENABLE_CHINESE_ANALYZER=-false}
ENABLE_HIVE_THRIFT_SERVER=${ENABLE_HIVE_THRIFT_SERVER=-true}

for env in MLSQL_SPARK_VERSION DRY_RUN DISTRIBUTION; do
  if [[ -z "${!env}" ]]; then
    echo "===$env must be set to run this script==="
    echo "===Please run ./dev/package.sh help to get how to use.==="
    exit 1
  fi
done

if [[ ${ENABLE_CHINESE_ANALYZER} = true && ! -f $SELF/../dev/ansj_seg-5.1.6.jar  && ! -f $SELF/../dev/nlp-lang-1.7.8.jar ]]
then
  echo "When ENABLE_CHINESE_ANALYZER=true, ansj_seg-5.1.6.jar && nlp-lang-1.7.8.jar should be in ./dev/"
  exit 1
fi

# before we compile and package, correct the version in MLSQLVersion
#---------------------

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     machine=Linux;;
    Darwin*)    machine=Mac;;
    CYGWIN*)    machine=Cygwin;;
    MINGW*)     machine=MinGw;;
    *)          machine="UNKNOWN:${unameOut}"
esac
echo ${machine}

current_version=$(cat pom.xml|grep -e '<version>.*</version>' | head -n 1 | tail -n 1 | cut -d'>' -f2 | cut -d '<' -f1)
MLSQL_VERSION_FILE="./streamingpro-core/src/main/java/tech/mlsql/core/version/MLSQLVersion.scala"

if [[ "${machine}" == "Linux" ]]
then
    sed -i "s/MLSQL_VERSION_PLACEHOLDER/${current_version}/" ${MLSQL_VERSION_FILE}
elif [[ "${machine}" == "Mac" ]]
then
    sed -i '' "s/MLSQL_VERSION_PLACEHOLDER/${current_version}/" ${MLSQL_VERSION_FILE}
else
 echo "Windows  is not supported yet"
 exit 0
fi
#---------------------

BASE_PROFILES="-Ponline  "

if [[ "${ENABLE_HIVE_THRIFT_SERVER}" == "true" ]]; then
  BASE_PROFILES="$BASE_PROFILES -Phive-thrift-server"
fi

if [[ "${ENABLE_JYTHON}" == "true" ]]; then
  BASE_PROFILES="$BASE_PROFILES -Pjython-support"
fi

if [[ "${ENABLE_CHINESE_ANALYZER}" == "true" ]]; then
  BASE_PROFILES="$BASE_PROFILES -Pchinese-analyzer-support"
fi


if [[ "$MLSQL_SPARK_VERSION" == "2.4" ]]; then
  BASE_PROFILES="$BASE_PROFILES -Pscala-2.11"
else
  BASE_PROFILES="$BASE_PROFILES -Pscala-2.12"
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

if [[ "${COMMAND}" == "package" || "${COMMAND}" == "install" || "${COMMAND}" == "deploy" ]];then
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
   BASE_PROFILES="$BASE_PROFILES -Paliyun-oss"
fi

if [[ "$DATASOURCE_INCLUDED" == "true" ]];then
   BASE_PROFILES="$BASE_PROFILES -Punit-test"
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


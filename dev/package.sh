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

set -e
set -o pipefail

BASE_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)

function exit_with_usage {
  cat << EOF
usage: package
run package command based on spark 3.3.3
DRY_RUN true|false               default false
DISTRIBUTION true|false          default false
DATASOURCE_INCLUDED true|false   default false
EOF
  exit 1
}

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

cd "$BASE_DIR" || exit 1

DRY_RUN=${DRY_RUN:-false}
DISTRIBUTION=${DISTRIBUTION:-false}
DATASOURCE_INCLUDED=${DATASOURCE_INCLUDED:-false}
COMMAND=${COMMAND:-package}

ENABLE_JYTHON=${ENABLE_JYTHON=-false}
ENABLE_CHINESE_ANALYZER=${ENABLE_CHINESE_ANALYZER=-false}
ENABLE_HIVE_THRIFT_SERVER=${ENABLE_HIVE_THRIFT_SERVER=-true}


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
echo "${machine}"

current_version=$(cat pom.xml|grep -e '<version>.*</version>' | head -n 1 | tail -n 1 | cut -d'>' -f2 | cut -d '<' -f1)

echo "Parse the version of byzer lang, the version is:""$current_version"

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
BASE_PROFILES="-Ponline"

if [[ ${DISTRIBUTION} == "true" ]];then
  BASE_PROFILES="${BASE_PROFILES} -Passembly"
else
  BASE_PROFILES="${BASE_PROFILES} -pl streamingpro-mlsql -am"
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
mvn clean ${COMMAND} ${SKIPTEST} ${BASE_PROFILES}  ${TESTPROFILE}
EOF

else
cat << EOF
mvn clean ${COMMAND} ${SKIPTEST} ${BASE_PROFILES}  ${TESTPROFILE}
EOF
mvn clean ${COMMAND} ${SKIPTEST} ${BASE_PROFILES} ${TESTPROFILE}
fi

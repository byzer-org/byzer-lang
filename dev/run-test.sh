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

set -u
set -e
set -o pipefail

DEV_DIR=$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)

if [ -n "$1" ]; then
  export MLSQL_SPARK_VERSION=${1}
else
  export MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION:-3.0}
fi

if [ "${MLSQL_SPARK_VERSION}" == "3.0" ]; then
  if [ ! -f "${DEV_DIR}"/ansj_seg-5.1.6.jar ]; then
    wget --no-check-certificate --no-verbose "http://download.mlsql.tech/nlp/ansj_seg-5.1.6.jar" --directory-prefix "${DEV_DIR}/"
  fi
  if [ ! -f "${DEV_DIR}"/nlp-lang-1.7.8.jar ]; then
    wget --no-check-certificate --no-verbose http://download.mlsql.tech/nlp/nlp-lang-1.7.8.jar --directory-prefix "${DEV_DIR}/"
  fi
  echo "Download 3rd-party jars succeed"
  ./dev/make-distribution.sh

elif [ "${MLSQL_SPARK_VERSION}" == "2.4" ]; then
  ./dev/change-scala-version.sh 2.11
  python ./dev/python/convert_pom.py 2.4
else
  echo "Only accept 2.4|3.0"
  exit -1
fi

if [ "${SKIP_INSTALL:-}" != "skipInstall" ]; then
  mvn clean install -DskipTests
else
  mvn clean
fi

MATCHES=${2:-.*}

mvn test -pl streamingpro-it "-DargLine=-Dmatches=${MATCHES}"

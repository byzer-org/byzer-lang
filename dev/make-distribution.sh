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
Environment variables
MLSQL_SPARK_VERSION              Spark major version 2.3 2.4 3.0  default 2.4
OSS_ENABLE                       Aliyun OSS                       default false
ENABLE_JYTHON                    Jython                           default true
ENABLE_CHINESE_ANALYZER          Chinese NLP                      default true
ENABLE_HIVE_THRIFT_SERVER        Hive ThriftServer                default true
EOF
  exit 1
}

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

export LC_ALL=zh_CN.UTF-8
export LANG=zh_CN.UTF-8

## Spark major version
export MLSQL_SPARK_VERSION=${MLSQL_SPARK_VERSION:-2.4}
## Enable Aliyun OSS support, default to false
export OSS_ENABLE=${OSS_ENABLE:-false}
## Enable Jython support
export ENABLE_JYTHON=${ENABLE_JYTHON:-true}
## Including Chinese NLP jars
export ENABLE_CHINESE_ANALYZER=${ENABLE_CHINESE_ANALYZER:-true}
## Including Hive ThriftServe jars
export ENABLE_HIVE_THRIFT_SERVER=${ENABLE_HIVE_THRIFT_SERVER:-true}

## DATASOURCE_INCLUDED is for testing purposes only; therefore false
export DATASOURCE_INCLUDED=false

export DRY_RUN=false
## True means making a distribution package
export DISTRIBUTION=true

echo "Environment variables
MLSQL_SPARK_VERSION ${MLSQL_SPARK_VERSION}
OSS_ENABLE ${OSS_ENABLE}
ENABLE_JYTHON ${ENABLE_JYTHON}
ENABLE_CHINESE_ANALYZER ${ENABLE_CHINESE_ANALYZER}
ENABLE_HIVE_THRIFT_SERVER ${ENABLE_HIVE_THRIFT_SERVER}"

SELF=$(cd $(dirname $0) && pwd)
cd $SELF

if [[ ${MLSQL_SPARK_VERSION} = "2.3" || ${MLSQL_SPARK_VERSION} = "2.4" ]]
then
  ./change-scala-version.sh 2.11
elif [[ ${MLSQL_SPARK_VERSION} = "3.0" ]]
then
  ./change-scala-version.sh 2.12
else
  echo "Spark-${MLSQL_SPARK_VERSION} is not supported"
  exit_with_usage
  exit 1
fi

## Start building
./package.sh

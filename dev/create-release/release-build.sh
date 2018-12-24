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

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

function exit_with_usage {
  cat << EOF
usage: release-build.sh <package|docs>
Creates build deliverables from a Spark commit.
Top level targets are
  package: Create binary packages and commit them to dist.apache.org/repos/dist/dev/spark/
  docs: Build docs and commit them to dist.apache.org/repos/dist/dev/spark/
All other inputs are environment variables
GIT_REF - Release tag or commit to build from
MLSQL_PACKAGE_VERSION - Release identifier in top level package directory (e.g. 2.1.2-rc1)
MLSQL_VERSION - (optional) Version of Spark being built (e.g. 2.1.2)
MLSQL_SPARK_VERSIOIN  e.g. 2.3.0
MLSQL_BIG_SPARK_VERSIOIN  e.g. 2.3
AK
AKS
EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  exit_with_usage
fi

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}

BASE_DIR=$(pwd)

init_java
init_maven_sbt


for env in GIT_REF MLSQL_PACKAGE_VERSION MLSQL_VERSION MLSQL_SPARK_VERSIOIN MLSQL_BIG_SPARK_VERSIOIN AK AKS; do
  if [ -z "${!env}" ]; then
    echo "$env must be set to run this script"
    exit 1
  fi
done


rm -rf streamingpro
git clone "$MLSQL_GIT_REPO"
cd streamingpro
git checkout $GIT_REF
git_hash=`git rev-parse --short HEAD`
echo "Checked out MLSQL git hash $git_hash"

if [ -z "$MLSQL_VERSION" ]; then
  # Run $MVN in a separate command so that 'set -e' does the right thing.
  TMP=$(mktemp)
  $MVN help:evaluate -Dexpression=project.version > $TMP
  MLSQL_VERSION=$(cat $TMP | grep -v INFO | grep -v WARNING | grep -v Download)
  rm $TMP
fi

# Depending on the version being built, certain extra profiles need to be activated, and
# different versions of Scala are supported.
BASE_PROFILES="-Pscala-2.11 -Ponline -Phive-thrift-server -Pcarbondata -Pshade -Pcrawler "
PUBLISH_SCALA_2_10=0
SCALA_2_11_PROFILES=
if [[ $MLSQL_SPARK_VERSIOIN > "2.3" ]]; then
  BASE_PROFILES="$BASE_PROFILES -Pdsl -Pxgboost"
else
  BASE_PROFILES="$BASE_PROFILES -Pdsl-legacy"
fi

BASE_PROFILES="$BASE_PROFILES -Pspark-$MLSQL_SPARK_VERSIOIN -Pstreamingpro-spark-$MLSQL_SPARK_VERSIOIN-adaptor"

if [[ ! $MLSQL_SPARK_VERSIOIN < "2.2." ]]; then
  if [[ $JAVA_VERSION < "1.8." ]]; then
    echo "Java version $JAVA_VERSION is less than required 1.8 for 2.2+"
    echo "Please set JAVA_HOME correctly."
    exit 1
  fi
else
  if ! [[ $JAVA_VERSION =~ 1\.7\..* ]]; then
    if [ -z "$JAVA_7_HOME" ]; then
      echo "Java version $JAVA_VERSION is higher than required 1.7 for pre-2.2"
      echo "Please set JAVA_HOME correctly."
      exit 1
    else
      export JAVA_HOME="$JAVA_7_HOME"
    fi
  fi
fi

# This is a band-aid fix to avoid the failure of Maven nightly snapshot in some Jenkins
# machines by explicitly calling /usr/sbin/lsof. Please see SPARK-22377 and the discussion
# in its pull request.
LSOF=lsof
if ! hash $LSOF 2>/dev/null; then
  LSOF=/usr/sbin/lsof
fi

if [ -z "$MLSQL_PACKAGE_VERSION" ]; then
  $MLSQL_PACKAGE_VERSION="${MLSQL_VERSION}-$(date +%Y_%m_%d_%H_%M)-${git_hash}"
fi

DEST_DIR_NAME="$MLSQL_PACKAGE_VERSION"

git clean -d -f -x
rm .gitignore
rm -rf .git

if [[ "$1" == "package" ]]; then
  # Source and binary tarballs
  echo "Packaging"
  cat <<EOF
================
Maven build command:

mvn -DskipTests clean package -pl streamingpro-mlsql -am $BASE_PROFILES
================
EOF
  mvn -DskipTests clean package -pl streamingpro-mlsql -am $BASE_PROFILES
  cd ..
  PNAME="mlsql-spark_$MLSQL_BIG_SPARK_VERSIOIN-$MLSQL_PACKAGE_VERSION"
  mkdir -p $PNAME/libs
  cp streamingpro/streamingpro-mlsql/target/streamingpro-mlsql-spark_$MLSQL_BIG_SPARK_VERSIOIN-$MLSQL_VERSION.jar $PNAME/libs/
  cp ../start-local.sh $PNAME/
  tar czvf $PNAME.tar.gz $PNAME
  cd ../
  export MLSQL_RELEASE_TAR="./create-release/$PNAME.tar.gz"
  python -m mlsqltestssupport.aliyun.upload_release
  exit 0
fi

echo "ERROR: expects to be called with 'package', 'docs'"
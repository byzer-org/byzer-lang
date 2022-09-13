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

checkPython(){
    python_version=3
    user_py_version=`python -V 2>&1 | awk '{print $2}'`
    if [ "${user_py_version%%.*}" -lt $python_version ]; then
        echo Your python version is : $user_py_version
        echo 'Current python version is not supported! Requires python version is 3.x.x.'
        exit 1
    fi
}

checkPython

export LC_ALL=zh_CN.UTF-8
export LANG=zh_CN.UTF-8

## DATASOURCE_INCLUDED is for testing purposes only; therefore false
export DATASOURCE_INCLUDED=false

export DRY_RUN=false
## True means making a distribution package
export DISTRIBUTION=true

## Change directory to base directory
base=$(cd "$(dirname $0)/.." && pwd)
cd "$base/" || exit 1

## Start building
./dev/package.sh

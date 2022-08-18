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

#title=Checking Java Version

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Java version..."

$JAVA -version 2>&1 || quit "ERROR: Detect java version failed. Please set JAVA_HOME."

if [[ $(isValidJavaVersion) == "false" ]]; then
    quit "ERROR: Java 1.8 or above is required for Byzer engine, current java is ${JAVA}"
fi

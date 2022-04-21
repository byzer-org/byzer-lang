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

#title=Checking Ports Availability

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

byzer_lang_port=`$BYZER_HOME/bin/get-properties.sh streaming.driver.port`
if [[ -z ${byzer_lang_port} ]]; then
    byzer_lang_port=9003
fi
if [[ $MACHINE_OS == "Linux" ]]; then
    byzer_lang_port_in_use=`netstat -nat | grep "${byzer_lang_port}"`
fi
if [[ $MACHINE_OS == "Mac" ]]; then
    byzer_lang_port_in_use=`lsof -nP -iTCP:${byzer_lang_port} -sTCP:LISTEN`
fi

[[ -z ${byzer_lang_port_in_use} ]] || quit "ERROR: Port ${byzer_lang_port} is in use, another Byzer-lang is running?"

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.sparkcube.conf

import org.apache.spark.sql.internal.SQLConf

object CubeConf {

  val QUERY_REWRITE_ENABLED = SQLConf.buildConf("spark.sql.cache.queryRewrite")
    .doc("When true, spark try to use previous built Cube Management to optimized input queries.")
    .booleanConf
    .createWithDefault(true)

  val RELATIONAL_META_STORE_PATH = SQLConf.buildConf("spark.sql.cache.meta.storage.path")
    .doc("Local level db location to store some Cube Management metadata.")
    .stringConf
    .createWithDefault(System.getProperty("user.dir"))

  val DATA_SKIP_ENABLED = SQLConf.buildConf("spark.sql.cube.dataskip.enabled")
    .doc("Whether enable data skipping when building/scanning cube.")
    .booleanConf
    .createWithDefault(true)

}

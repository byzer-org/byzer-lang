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

package com.alibaba.sparkcube.execution.api

case class CacheBasicInfo(
    viewDatabase: Option[String],
    viewName: String,
    cacheName: String,
    enableRewrite: Boolean,
    cacheSize: String,
    lastUpdateTime: String)

trait CacheDetailInfo

case class RawCacheDetailInfo(
    viewDatabase: Option[String],
    viewName: String,
    cacheName: String,
    enableRewrite: Boolean,
    cacheSize: String,
    lastUpdateTime: String,
    cacheType: String,
    selectCols: Seq[String],
    location: String,
    provider: String,
    partitionBy: Seq[String],
    zorderBy: Seq[String]) extends CacheDetailInfo

case class CubeCacheDetailInfo(
    viewDatabase: Option[String],
    viewName: String,
    cacheName: String,
    enableRewrite: Boolean,
    cacheSize: String,
    lastUpdateTime: String,
    cacheType: String,
    dimensions: Seq[String],
    measures: Seq[String],
    location: String,
    provider: String,
    partitionBy: Seq[String],
    zorderBy: Seq[String]) extends CacheDetailInfo

case class CachePartitionInfo(path: String, size: String)

case class ActionResponse(status: String, message: String)

case class EnableParam(enable: Boolean)

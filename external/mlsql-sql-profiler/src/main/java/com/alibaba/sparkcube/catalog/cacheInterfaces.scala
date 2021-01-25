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

package com.alibaba.sparkcube.catalog

import org.apache.spark.sql.catalyst.catalog.BucketSpec

import com.alibaba.sparkcube.optimizer._

case class CacheStorageInfo(
    storagePath: String,
    provider: String = "PARQUET",
    partitionSpec: Option[Seq[String]] = None,
    zorder: Option[Seq[String]] = None,
    bucketSpec: Option[BucketSpec] = None)

case class CacheInfo(rawCacheInfo: Option[RawCacheInfo], cubeCacheInfo: Option[CubeCacheInfo]) {
  def isEmpty(): Boolean = rawCacheInfo.isEmpty && cubeCacheInfo.isEmpty
}

case class RawCacheInfo(
    cacheName: String,
    enableRewrite: Boolean,
    storageInfo: CacheStorageInfo,
    cacheSchema: CacheRawSchema,
    lastUpdateTime: Long) extends BasicCacheInfo(cacheName, enableRewrite,
  storageInfo, cacheSchema, lastUpdateTime)

case class CubeCacheInfo(
    cacheName: String,
    enableRewrite: Boolean,
    storageInfo: CacheStorageInfo,
    cacheSchema: CacheCubeSchema,
    lastUpdateTime: Long) extends BasicCacheInfo(cacheName, enableRewrite,
  storageInfo, cacheSchema, lastUpdateTime)

sealed abstract class BasicCacheInfo(
    cacheName: String,
    enableRewrite: Boolean,
    storageInfo: CacheStorageInfo,
    cacheSchema: CacheSchema,
    lastUpdateTime: Long) extends Serializable {
  def getCacheName: String = cacheName

  def getStorageInfo: CacheStorageInfo = storageInfo

  def getCacheSchema: CacheSchema = cacheSchema

  def getEnableRewrite: Boolean = enableRewrite

  def getLastUpdateTime: Long = lastUpdateTime
}


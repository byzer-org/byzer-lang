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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * cube catalog
 */
trait CubeExternalCatalog {

  def isCached(db: String, mv: String): Boolean

  def listSparkCubes(db: String): Seq[CatalogTable]

  def listSparkCubes(db: String, pattern: String): Seq[CatalogTable]

  def getCacheInfo(table: TableIdentifier): Option[CacheInfo]

  def setCacheInfo(table: TableIdentifier, cacheInfo: CacheInfo): Unit

  def clearCacheInfo(table: TableIdentifier): Unit

}

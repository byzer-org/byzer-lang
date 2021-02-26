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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalog}
import org.apache.spark.sql.catalyst.util.StringUtils


/**
 * If using in-memory catalog in Spark, we will not invoke hive metastore
 */
class CubeInMemoryCatalog(inMemoryCatalog: ExternalCatalog) extends CubeExternalCatalog {

  private val cubeCaches = new mutable.HashMap[String, CacheInfo]

  protected def requireDbExists(db: String): Unit = {
    if (!inMemoryCatalog.databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }

  override def isCached(db: String, view: String): Boolean = synchronized {
    if (inMemoryCatalog.tableExists(db, view)) {
      cubeCaches.get(inMemoryCatalog.getTable(db, view).qualifiedName).nonEmpty
    } else {
      false
    }
  }

  override def listSparkCubes(db: String): Seq[CatalogTable] = synchronized {
    requireDbExists(db)
    inMemoryCatalog.listTables(db)
      .filter(getCacheInfo(db, _).nonEmpty).map(inMemoryCatalog.getTable(db, _))
  }

  override def listSparkCubes(db: String, pattern: String): Seq[CatalogTable] =
    synchronized {
      requireDbExists(db)
      StringUtils.filterPattern(inMemoryCatalog.listTables(db), pattern)
        .filter(getCacheInfo(db, _).nonEmpty).map(inMemoryCatalog.getTable(db, _))
    }

  private def getCacheInfo(db: String, mv: String): Option[CacheInfo] = {
    cubeCaches.get(TableIdentifier(mv, Some(db)).unquotedString)
  }

  override def getCacheInfo(table: TableIdentifier): Option[CacheInfo] = {
    cubeCaches.get(table.unquotedString)
  }

  override def setCacheInfo(table: TableIdentifier, cacheInfo: CacheInfo): Unit = {
    cubeCaches.update(table.unquotedString, cacheInfo)
  }

  override def clearCacheInfo(table: TableIdentifier): Unit = {
    cubeCaches.remove(table.unquotedString)
  }
}

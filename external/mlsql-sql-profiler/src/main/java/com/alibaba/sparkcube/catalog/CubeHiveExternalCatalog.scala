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
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, ExternalCatalog}

import com.alibaba.sparkcube.optimizer._


/**
 * Use Hive metastore as catalog
 */
class CubeHiveExternalCatalog(
    hiveCatalog: ExternalCatalog) extends CubeExternalCatalog {

  protected def requireTableExists(db: String, table: String): Unit = {
    if (!hiveCatalog.tableExists(db, table)) {
      throw new NoSuchTableException(db = db, table = table)
    }
  }

  override def listSparkCubes(db: String): Seq[CatalogTable] = {
    hiveCatalog.listTables(db).map(t =>
      (hiveCatalog.getTable(db, t), getCacheInfo(db, t))).filter(_._2.isDefined).map(_._1)
  }

  override def listSparkCubes(db: String, pattern: String): Seq[CatalogTable] = {
    hiveCatalog.listTables(db, pattern).map(t =>
      (hiveCatalog.getTable(db, t), getCacheInfo(db, t))).filter(_._2.isDefined).map(_._1)
  }

  override def isCached(db: String, mv: String): Boolean = {
    requireTableExists(db, mv)
    propertiesToCacheInfo(hiveCatalog.getTable(db, mv).properties).nonEmpty
  }

  private def cacheInfoToProperties(cacheInfo: Option[CacheInfo]): Map[String, String] = {
    val cacheProperties = new mutable.HashMap[String, String]()
    cacheInfo match {
      case Some(cache) =>
        cacheProperties += CubeHiveExternalCatalog.CACHED -> "true"
        if (cache.rawCacheInfo.isDefined) {
          val rawCache = cache.rawCacheInfo.get

          cacheProperties +=
            CubeHiveExternalCatalog.RAW_CACHE_PREFIX -> rawCache.cacheName
          cacheProperties +=
            CubeHiveExternalCatalog.RAW_CACHE_ENABLE_REWRITE -> rawCache.enableRewrite.toString
          cacheProperties +=
            CubeHiveExternalCatalog.RAW_CACHE_COLUMNS -> rawCache.cacheSchema.cols.mkString(",")
          cacheProperties +=
            CubeHiveExternalCatalog.RAW_CACHE_LAST_UPDATED_TIME -> rawCache.lastUpdateTime.toString
          cacheProperties +=
            CubeHiveExternalCatalog.RAW_CACHE_STORAGE_PATH -> rawCache.storageInfo.storagePath
          cacheProperties +=
            CubeHiveExternalCatalog.RAW_CACHE_PROVIDER -> rawCache.storageInfo.provider
          if (rawCache.storageInfo.partitionSpec.isDefined &&
            rawCache.storageInfo.partitionSpec.get.nonEmpty) {
            cacheProperties += CubeHiveExternalCatalog.RAW_CACHE_PARTITION ->
              rawCache.storageInfo.partitionSpec.get.mkString(",")
          }
          if (rawCache.storageInfo.zorder.isDefined &&
            rawCache.storageInfo.zorder.get.nonEmpty) {
            cacheProperties += CubeHiveExternalCatalog.RAW_CACHE_ZORDER ->
              rawCache.storageInfo.zorder.get.mkString(",")
          }
          if (rawCache.storageInfo.bucketSpec.isDefined) {
            val bucketSpec = rawCache.storageInfo.bucketSpec.get
            cacheProperties += CubeHiveExternalCatalog.RAW_CACHE_BUCKET_PREFIX -> "true"
            cacheProperties += CubeHiveExternalCatalog.RAW_CACHE_BUCKET_COLUMNS ->
              bucketSpec.bucketColumnNames.mkString(",")
            cacheProperties += CubeHiveExternalCatalog.RAW_CACHE_BUCKET_NUMBER ->
              bucketSpec.numBuckets.toString
            cacheProperties += CubeHiveExternalCatalog.RAW_CACHE_BUCKET_SORT_COLUMNS ->
              bucketSpec.sortColumnNames.mkString(",")
          }
        }
        if (cache.cubeCacheInfo.isDefined) {
          val cubeCache = cache.cubeCacheInfo.get

          cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_PREFIX ->
            cubeCache.cacheName
          cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_ENABLE_REWRITE ->
            cubeCache.enableRewrite.toString
          cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_DIMS ->
            cubeCache.cacheSchema.dims.mkString(",")
          val measureStr = cubeCache.cacheSchema.measures.map(_.toString).mkString(",")
          cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_MEASURES ->
            measureStr
          cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_LAST_UPDATED_TIME ->
            cubeCache.lastUpdateTime.toString
          cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_STORAGE_PATH ->
            cubeCache.storageInfo.storagePath
          cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_PROVIDER ->
            cubeCache.storageInfo.provider
          if (cubeCache.storageInfo.partitionSpec.isDefined) {
            cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_PARTITION ->
              cubeCache.storageInfo.partitionSpec.get.mkString(",")
          }
          if (cubeCache.storageInfo.zorder.isDefined &&
            cubeCache.storageInfo.zorder.get.nonEmpty) {
            cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_ZORDER ->
              cubeCache.storageInfo.zorder.get.mkString(",")
          }
          if (cubeCache.storageInfo.bucketSpec.isDefined) {
            val bucketSpec = cubeCache.storageInfo.bucketSpec.get
            cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_BUCKET_PREFIX -> "true"
            cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_BUCKET_COLUMNS ->
              bucketSpec.bucketColumnNames.mkString(",")
            cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_BUCKET_NUMBER ->
              bucketSpec.numBuckets.toString
            cacheProperties += CubeHiveExternalCatalog.CUBE_CACHE_BUCKET_SORT_COLUMNS ->
              bucketSpec.sortColumnNames.mkString(",")
          }
        }
      case None =>
        cacheProperties += CubeHiveExternalCatalog.CACHED -> "false"
    }
    cacheProperties.toMap
  }

  private def propertiesToCacheInfo(properties: Map[String, String]): Option[CacheInfo] = {
    val isCached = properties.getOrElse(CubeHiveExternalCatalog.CACHED, "false").toBoolean
    if (isCached) {
      val rawCacheInfo = if (properties.contains(CubeHiveExternalCatalog.RAW_CACHE_PREFIX)) {
        val rawCacheName = properties(CubeHiveExternalCatalog.RAW_CACHE_PREFIX)
        val enableRewrite = properties(CubeHiveExternalCatalog.RAW_CACHE_ENABLE_REWRITE).toBoolean
        val storagePath = properties(CubeHiveExternalCatalog.RAW_CACHE_STORAGE_PATH)
        val provider = properties(CubeHiveExternalCatalog.RAW_CACHE_PROVIDER)
        val partitionCols = properties.get(CubeHiveExternalCatalog.RAW_CACHE_PARTITION) match {
          case Some(partStr) =>
            Some(partStr.split(",").toSeq)
          case _ =>
            None
        }
        val zorderCols = properties.get(CubeHiveExternalCatalog.RAW_CACHE_ZORDER) match {
          case Some(zorderStr) =>
            Some(zorderStr.split(",").toSeq)
          case _ =>
            None
        }
        val bucketSpec = properties.get(CubeHiveExternalCatalog.RAW_CACHE_BUCKET_PREFIX) match {
          case Some("true") =>
            val bucketNumber = properties(CubeHiveExternalCatalog.RAW_CACHE_BUCKET_NUMBER).toInt
            val bucketColumns =
              properties(CubeHiveExternalCatalog.RAW_CACHE_BUCKET_COLUMNS).split(",")
            val bucketSortColumns =
              properties(CubeHiveExternalCatalog.RAW_CACHE_BUCKET_SORT_COLUMNS).split(",")
            Some(BucketSpec(bucketNumber, bucketColumns, bucketSortColumns))
          case Some("false") =>
            None
          case _ =>
            None
        }
        val storageInfo =
          CacheStorageInfo(storagePath, provider, partitionCols, zorderCols, bucketSpec)
        val cacheSchema =
          CacheRawSchema(properties(CubeHiveExternalCatalog.RAW_CACHE_COLUMNS).split(","))
        val lastUpdatedTime =
          properties(CubeHiveExternalCatalog.RAW_CACHE_LAST_UPDATED_TIME).toLong
        Some(RawCacheInfo(rawCacheName, enableRewrite, storageInfo, cacheSchema, lastUpdatedTime))
      } else {
        None
      }

      val cubeCacheInfo = if (properties.contains(CubeHiveExternalCatalog.CUBE_CACHE_PREFIX)) {
        val cubeCacheName = properties(CubeHiveExternalCatalog.CUBE_CACHE_PREFIX)
        val enableRewrite = properties(CubeHiveExternalCatalog.CUBE_CACHE_ENABLE_REWRITE).toBoolean
        val storagePath = properties(CubeHiveExternalCatalog.CUBE_CACHE_STORAGE_PATH)
        val provider = properties(CubeHiveExternalCatalog.CUBE_CACHE_PROVIDER)
        val partitionCols = properties.get(CubeHiveExternalCatalog.CUBE_CACHE_PARTITION) match {
          case Some(partStr) =>
            Some(partStr.split(",").toSeq)
          case _ =>
            None
        }
        val zorderCols = properties.get(CubeHiveExternalCatalog.CUBE_CACHE_ZORDER) match {
          case Some(zorderStr) =>
            Some(zorderStr.split(",").toSeq)
          case _ =>
            None
        }
        val bucketSpec = properties.get(CubeHiveExternalCatalog.CUBE_CACHE_BUCKET_PREFIX) match {
          case Some("true") =>
            val bucketNumber = properties(CubeHiveExternalCatalog.CUBE_CACHE_BUCKET_NUMBER).toInt
            val bucketColumns =
              properties(CubeHiveExternalCatalog.CUBE_CACHE_BUCKET_COLUMNS).split(",")
            val bucketSortColumns =
              properties(CubeHiveExternalCatalog.CUBE_CACHE_BUCKET_SORT_COLUMNS).split(",")
            Some(BucketSpec(bucketNumber, bucketColumns, bucketSortColumns))
          case Some("false") =>
            None
          case _ =>
            None
        }
        val storageInfo = CacheStorageInfo(storagePath, provider, partitionCols, zorderCols,
          bucketSpec)
        val cubeDims = properties(CubeHiveExternalCatalog.CUBE_CACHE_DIMS).split(",")
        val cubeMeasures = properties(CubeHiveExternalCatalog.CUBE_CACHE_MEASURES).split(",").map(
          Measure.apply)
        val cacheSchema = CacheCubeSchema(cubeDims, cubeMeasures)
        val lastUpdatedTime =
          properties(CubeHiveExternalCatalog.CUBE_CACHE_LAST_UPDATED_TIME).toLong
        Some(CubeCacheInfo(cubeCacheName, enableRewrite, storageInfo, cacheSchema, lastUpdatedTime))
      } else {
        None
      }
      Some(CacheInfo(rawCacheInfo, cubeCacheInfo))
    } else {
      None
    }
  }

  private def getCacheInfo(db: String, mv: String): Option[CacheInfo] = {
    propertiesToCacheInfo(hiveCatalog.getTable(db, mv).properties)
  }

  override def getCacheInfo(table: TableIdentifier): Option[CacheInfo] = {
    propertiesToCacheInfo(hiveCatalog.getTable(table.database.get, table.table).properties)
  }

  override def setCacheInfo(table: TableIdentifier, cacheInfo: CacheInfo): Unit = {
    val oldTable = hiveCatalog.getTable(table.database.get, table.table)
    val updatedTable =
      oldTable.copy(properties = oldTable.properties ++ cacheInfoToProperties(Some(cacheInfo)))
    hiveCatalog.alterTable(updatedTable)
  }

  override def clearCacheInfo(table: TableIdentifier): Unit = {
    val oldTable = hiveCatalog.getTable(table.database.get, table.table)
    val oldCache = propertiesToCacheInfo(oldTable.properties)
    oldCache match {
      case Some(_) =>
        val removeProp = oldTable.properties -- cacheInfoToProperties(oldCache).keys
        val updateProp = removeProp ++ cacheInfoToProperties(None)
        hiveCatalog.alterTable(oldTable.copy(properties = updateProp))
      case None =>
    }
  }
}

object CubeHiveExternalCatalog {

  val SPARK_SQL_PREFIX = "spark.cube."
  val CACHED = SPARK_SQL_PREFIX + "cached"
  val CACHED_INFO = SPARK_SQL_PREFIX + "cached.info"
  val RAW_CACHE_PREFIX = CACHED_INFO + ".raw"
  val RAW_CACHE_ENABLE_REWRITE = RAW_CACHE_PREFIX + ".enableRewrite"
  val RAW_CACHE_COLUMNS = RAW_CACHE_PREFIX + ".cacheColumns"
  val RAW_CACHE_LAST_UPDATED_TIME = RAW_CACHE_PREFIX + ".lastUpdatedTime"
  val RAW_CACHE_STORAGE_PATH = RAW_CACHE_PREFIX + ".storagePath"
  val RAW_CACHE_PROVIDER = RAW_CACHE_PREFIX + ".provider"
  val RAW_CACHE_PARTITION = RAW_CACHE_PREFIX + ".partitionBy"
  val RAW_CACHE_ZORDER = RAW_CACHE_PREFIX + ".zorderBy"
  val RAW_CACHE_BUCKET_PREFIX = RAW_CACHE_PARTITION + ".bucketSpec"
  val RAW_CACHE_BUCKET_NUMBER = RAW_CACHE_BUCKET_PREFIX + ".number"
  val RAW_CACHE_BUCKET_COLUMNS = RAW_CACHE_BUCKET_PREFIX + ".columns"
  val RAW_CACHE_BUCKET_SORT_COLUMNS = RAW_CACHE_BUCKET_PREFIX + ".sort.columns"
  val CUBE_CACHE_PREFIX = CACHED_INFO + ".cube"
  val CUBE_CACHE_ENABLE_REWRITE = CUBE_CACHE_PREFIX + ".enableRewrite"
  val CUBE_CACHE_DIMS = CUBE_CACHE_PREFIX + ".dims"
  val CUBE_CACHE_MEASURES = CUBE_CACHE_PREFIX + ".measures"
  val CUBE_CACHE_LAST_UPDATED_TIME = CUBE_CACHE_PREFIX + ".lastUpdatedTime"
  val CUBE_CACHE_STORAGE_PATH = CUBE_CACHE_PREFIX + ".storagePath"
  val CUBE_CACHE_PROVIDER = CUBE_CACHE_PREFIX + ".provider"
  val CUBE_CACHE_PARTITION = CUBE_CACHE_PREFIX + ".partitionBy"
  val CUBE_CACHE_ZORDER = CUBE_CACHE_PREFIX + ".zorderBy"
  val CUBE_CACHE_BUCKET_PREFIX = CUBE_CACHE_PREFIX + ".bucketSpec"
  val CUBE_CACHE_BUCKET_NUMBER = CUBE_CACHE_BUCKET_PREFIX + ".number"
  val CUBE_CACHE_BUCKET_COLUMNS = CUBE_CACHE_BUCKET_PREFIX + ".columns"
  val CUBE_CACHE_BUCKET_SORT_COLUMNS = CUBE_CACHE_BUCKET_PREFIX + ".sort.columns"
}

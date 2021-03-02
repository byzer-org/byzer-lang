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

package com.alibaba.sparkcube

import java.util.UUID
import java.util.concurrent._

import com.alibaba.sparkcube.catalog._
import com.alibaba.sparkcube.conf.CubeConf
import com.alibaba.sparkcube.execution._
import com.alibaba.sparkcube.optimizer._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions.{col, lit, max, row_number}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, _}

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using any spark supported
 * DataSource. This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 *
 * Internal to Spark SQL.
 */
class CubeManager extends Logging {

  private val RAW_CACHE_PATH = "raw"
  private val CUBE_CACHE_PATH = "cube"

  private val cacheBuildTaskFutures =
    new ConcurrentHashMap[CacheIdentifier, (PeriodBuildInfo, ScheduledFuture[_])]()
  private val buildingCaches = new ConcurrentSkipListSet[CacheIdentifier]()
  private val buildCacheHistories = new ConcurrentHashMap[CacheIdentifier, Seq[BuildHistory]]()
  private val cacheBuildExecutor = Executors.newScheduledThreadPool(5,
    new ThreadFactoryBuilder()
      .setNameFormat("SparkCubeBuilder-%s")
      .build())

  /**
   * Create Cube Management, add cache metadata to view properties in external catalog.
   *
   * @param viewName  The table/view name which represent the data to cache.
   * @param cacheInfo Cache data metadata information(location, format, partition, order, ...).
   */
  def createCache(
                   session: SparkSession,
                   viewName: String,
                   cacheInfo: CacheFormatInfo): Boolean = {
    if (isCacheExists(session, viewName, cacheInfo)) {
      logWarning(s"Cache already exists for $viewName.")
      false
    } else {
      val dataset = session.table(viewName)
      verifyCacheFormatInfo(cacheInfo, dataset.schema)
      verifyCachePlan(SparkAgent.getLogicalPlan(dataset))
      val tableIdentifier = parseTableName(session, viewName)
      val dbName = tableIdentifier.database.getOrElse("temp_view")
      //这里需要改成可配置的，比如数据湖位置
      val defaultPath = new Path(getCacheDefaultPath(session, dbName), tableIdentifier.table)
      val cacheStoragePath = cacheInfo.cacheSchema match {
        case _: CacheRawSchema => new Path(defaultPath, RAW_CACHE_PATH).toString
        case _: CacheCubeSchema => new Path(defaultPath, CUBE_CACHE_PATH).toString
      }
      val storageInfo = CacheStorageInfo(cacheStoragePath, cacheInfo.provider,
        cacheInfo.partitionColumns, cacheInfo.zorderColumns, cacheInfo.bucketSpec)
      addCacheMetaInExternalCatalog(session, tableIdentifier, cacheInfo, storageInfo)
      logInfo(s"Create Cube Management $viewName with: $cacheInfo")
      true
    }
  }

  /**
   * Build Cube Management data, persist cache data to spark datasource, new view data would
   * append to exists cache data.
   *
   * @param cacheId   cache identifier.
   * @param buildInfo Optional, buildInfo would be transformed to a filter condition on view data,
   *                  which represent new partial/incremental view data.
   */
  def buildCache(
                  session: SparkSession,
                  cacheId: CacheIdentifier,
                  buildInfo: Option[BuildInfo] = None): Unit = {
    buildInfo.map { info =>
      val viewSchema = session.sessionState.catalog
        .getTableMetadata(TableIdentifier(cacheId.viewName, cacheId.db)).schema
      info.verify(cacheId, viewSchema)
    }
    doCacheBuild(session, cacheId, buildInfo, SaveMode.Append)
  }

  /**
   * Asynchronously build cache data.
   *
   * @param session
   * @param cacheId
   * @param buildInfo
   */
  def asyncBuildCache(
                       session: SparkSession,
                       cacheId: CacheIdentifier,
                       buildInfo: Option[BuildInfo] = None): Unit = {
    cacheBuildExecutor.execute(new Runnable {
      override def run(): Unit = {
        buildCache(session, cacheId, buildInfo)
      }
    })
  }

  /**
   * Refresh Cube Management data, persist view data to spark datasource, new view data would
   * overwrite all exists cache data.
   *
   * @param cacheId   cache identifier.
   * @param buildInfo Optional, buildInfo would be transformed to a filter condition on view data.
   */
  def refreshCache(
                    session: SparkSession,
                    cacheId: CacheIdentifier,
                    buildInfo: Option[BuildInfo] = None): Unit = {
    buildInfo.map { info =>
      val viewSchema = session.sessionState.catalog
        .getTableMetadata(TableIdentifier(cacheId.viewName, cacheId.db)).schema
      info.verify(cacheId, viewSchema)
    }
    doCacheBuild(session, cacheId, buildInfo, SaveMode.Overwrite)
  }

  /**
   * Asynchronous refresh cache data.
   *
   * @param session
   * @param cacheId
   * @param buildInfo
   */
  def asyncRefreshCache(
                         session: SparkSession,
                         cacheId: CacheIdentifier,
                         buildInfo: Option[BuildInfo] = None): Unit = {
    cacheBuildExecutor.execute(new Runnable {
      override def run(): Unit = {
        refreshCache(session, cacheId, buildInfo)
      }
    })
  }

  /**
   * Build new cache data at a period of time with certain conditions, could be used to
   * update cache data incrementally and automatic. Column between [start, start + step) would be
   * used as the filter condition to represent incremental view data.
   */
  def autoBuildCache(
                      session: SparkSession,
                      cacheId: CacheIdentifier,
                      periodBuildInfo: PeriodBuildInfo): Unit = {
    val viewSchema = session.sessionState.catalog
      .getTableMetadata(TableIdentifier(cacheId.viewName, cacheId.db)).schema
    periodBuildInfo.verify(cacheId, viewSchema)
    doAutoBuildCache(session, cacheId, periodBuildInfo)
  }

  /**
   * Is cache enable automatic cache building.
   */
  def isCacheAutoBuild(cacheId: CacheIdentifier): Boolean = {
    cacheBuildTaskFutures.containsKey(cacheId)
  }

  /**
   * Cancel automatic cache building if enabled before.
   */
  def cancelAutoBuildCache(cacheId: CacheIdentifier): Unit = {
    if (cacheBuildTaskFutures.containsKey(cacheId)) {
      cacheBuildTaskFutures.remove(cacheId)._2.cancel(true)
      logInfo(s"Period automatic cache build for ${cacheId.toString} is canceled.")
    }
  }

  def getAutoBuildCache(cacheId: CacheIdentifier): Option[PeriodBuildInfo] = {
    if (cacheBuildTaskFutures.containsKey(cacheId)) {
      Some(cacheBuildTaskFutures.get(cacheId)._1)
    } else {
      None
    }
  }

  /**
   * Is cache under building now.
   */
  def isCacheUnderBuilding(cacheId: CacheIdentifier): Boolean = {
    buildingCaches.contains(cacheId)
  }

  protected def cubeCatalog(sparkSession: SparkSession): CubeExternalCatalog =
    CubeSharedState.get(sparkSession).cubeCatalog

  /**
   * List all Cube Managed view and its caches in current database.
   */
  def listCaches(session: SparkSession): Seq[(TableIdentifier, CacheInfo)] = {
    val db = session.sessionState.catalog.getCurrentDatabase
    val cCatalog = cubeCatalog(session)
    cCatalog.listSparkCubes(db).map(catalogTable =>
      (catalogTable.identifier, cCatalog.getCacheInfo(
        TableIdentifier(catalogTable.identifier.table, Some(db))).get))
  }

  /**
   * List all Cube Managed in catalog.
   */
  def listAllCaches(session: SparkSession): Seq[(TableIdentifier, CacheInfo)] = {
    val cCatalog = cubeCatalog(session)
    val useCacheDb = session.sparkContext.getConf.get("spark.sql.cache.useDatabase", "default")
      .split(",").toSeq
    useCacheDb.flatMap {
      db =>
        cCatalog.listSparkCubes(db).map { catalogTable =>
          (catalogTable.identifier, cCatalog.getCacheInfo(
            TableIdentifier(catalogTable.identifier.table, Some(db))).get)
        }
    }
  }

  /**
   * List cache build history information.
   */
  def listBuildHistory(
                        session: SparkSession,
                        cacheId: CacheIdentifier): Seq[BuildHistory] = {
    buildCacheHistories.getOrDefault(cacheId, Nil)
  }

  def clearBuildHistory(cacheId: Option[CacheIdentifier]): Unit = {
    cacheId match {
      case Some(cacheIdentifier) =>
        buildCacheHistories.remove(cacheIdentifier)
      case _ =>
        buildCacheHistories.clear()
    }
  }

  /**
   * Enable/Disable cache to be used for query rewriting.
   */
  def alterCacheRewrite(
                         spark: SparkSession,
                         cacheId: CacheIdentifier,
                         queryRewriteEnabled: Boolean): Unit = {
    val f = (cacheInfo: BasicCacheInfo) => cacheInfo match {
      case rawCacheInfo: RawCacheInfo => rawCacheInfo.copy(enableRewrite = queryRewriteEnabled)
      case cubeCacheInfo: CubeCacheInfo => cubeCacheInfo.copy(enableRewrite = queryRewriteEnabled)
    }
    alterCachePropertyInExternalCatalog(spark, cacheId, f)
    logInfo(s"Alter queryRewrite for ${cacheId.toString} to $queryRewriteEnabled")
  }

  /**
   * Clear existed cache, remove its metadata and cached data.
   */
  def dropCache(
                 spark: SparkSession,
                 cacheId: CacheIdentifier): Unit = {
    val viewIdentifier = TableIdentifier(cacheId.viewName, cacheId.db)
    getCacheInfo(spark, cacheId) match {
      case Some(basicCacheInfo) =>
        val cacheInfo = cubeCatalog(spark).getCacheInfo(viewIdentifier).get
        val updateInfo = basicCacheInfo match {
          case _: RawCacheInfo =>
            cacheInfo.copy(rawCacheInfo = None)
          case _: CubeCacheInfo =>
            cacheInfo.copy(cubeCacheInfo = None)
        }
        if (updateInfo.isEmpty) {
          cubeCatalog(spark).clearCacheInfo(viewIdentifier)
        } else {
          cubeCatalog(spark).setCacheInfo(viewIdentifier, updateInfo)
        }
        deleteCacheData(spark, basicCacheInfo.getStorageInfo.storagePath, None, true)
        clearRelatedInfo(cacheId)
        logInfo(s"Successfully drop cache of ${cacheId.toString}")
      case _ =>
        throw new CacheNotExistException(cacheId)
    }
  }

  /**
   * Clear all caches of input table/view.
   */
  def uncacheView(spark: SparkSession, viewName: String): Unit = {
    val viewIdentifier = parseTableName(spark, viewName)
    val cacheInfoOpt = cubeCatalog(spark).getCacheInfo(viewIdentifier)
    cacheInfoOpt match {
      case Some(cacheInfo) =>
        cubeCatalog(spark).clearCacheInfo(viewIdentifier)
        if (cacheInfo.rawCacheInfo.isDefined) {
          deleteCacheData(
            spark, cacheInfo.rawCacheInfo.get.storageInfo.storagePath, None, true)
          clearRelatedInfo(CacheIdentifier(viewIdentifier.database, viewIdentifier.table,
            cacheInfo.rawCacheInfo.get.cacheName))
        }
        if (cacheInfo.cubeCacheInfo.isDefined) {
          deleteCacheData(
            spark, cacheInfo.cubeCacheInfo.get.storageInfo.storagePath, None, true)
          clearRelatedInfo(CacheIdentifier(viewIdentifier.database, viewIdentifier.table,
            cacheInfo.cubeCacheInfo.get.cacheName))
        }
        logInfo(s"Successfully drop all caches of $viewName")
      case None =>
        throw new NotCachedException(viewIdentifier.database.getOrElse(""), viewIdentifier.table)
    }
  }

  /**
   * Drop specified cache partition data if cache data is partitioned, could be used to drop
   * useless cache data partitions.
   */
  def dropCachePartition(
                          spark: SparkSession,
                          cacheId: CacheIdentifier,
                          specs: Seq[String]): Unit = {
    getCacheInfo(spark, cacheId) match {
      case Some(basicCacheInfo) =>
        specs.foreach { partitionPath =>
          deleteCacheData(
            spark, basicCacheInfo.getStorageInfo.storagePath, Some(partitionPath), true)
        }
        logInfo(s"Successfully drop cache partition(${specs.mkString(",")}) of" +
          s" ${cacheId.toString}")
      case _ =>
        throw new CacheNotExistException(cacheId)
    }
  }

  /**
   * List all cache data partitions if cache data is partitioned.
   */
  def listCachePartitions(
                           spark: SparkSession,
                           cacheId: CacheIdentifier): Seq[String] = {
    getCacheInfo(spark, cacheId) match {
      case Some(basicCacheInfo) =>
        val storageInfo = basicCacheInfo.getStorageInfo
        val partitionCols = storageInfo.partitionSpec
        val path = new Path(storageInfo.storagePath)
        val fs = path.getFileSystem(spark.sessionState.newHadoopConf)
        if (fs.exists(path) && partitionCols.isDefined && partitionCols.get.nonEmpty) {
          var partitionPaths = Seq(path)
          for (_ <- (0 until partitionCols.get.size)) {
            partitionPaths = partitionPaths.flatMap(fs.listStatus(_).map(_.getPath))
          }

          partitionPaths.map(_.toUri.getPath.stripPrefix(path.toUri.getPath + "/"))
            .filterNot(shouldFilterOut)
        } else {
          Seq.empty
        }
      case _ => Seq.empty
    }
  }

  /**
   * Is table/view cached.
   */
  def isCached(spark: SparkSession, tableName: String): Boolean = {
    val cache = parseTableName(spark, tableName)
    cubeCatalog(spark).isCached(cache.database.getOrElse("temp_view"), cache.table)
  }

  def getViewCacheInfo(spark: SparkSession, viewName: String): Option[CacheInfo] = {
    val view = parseTableName(spark, viewName)
    cubeCatalog(spark).getCacheInfo(view)
  }

  def getCacheInfo(
                    spark: SparkSession,
                    cacheIdentifier: CacheIdentifier): Option[BasicCacheInfo] = {
    cubeCatalog(spark).getCacheInfo(cacheIdentifier) match {
      case Some(cacheInfo) =>
        if (cacheInfo.rawCacheInfo.isDefined &&
          cacheInfo.rawCacheInfo.get.cacheName == cacheIdentifier.cacheName) {
          cacheInfo.rawCacheInfo
        } else if (cacheInfo.cubeCacheInfo.isDefined &&
          cacheInfo.cubeCacheInfo.get.cacheName == cacheIdentifier.cacheName) {
          cacheInfo.cubeCacheInfo
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * Try to use cache data to optimize logical plan of user query, it's invoked between analyzer and
   * optimizer.
   */
  def useCachedData(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _: LocalRelation => plan
      case _ if SQLConf.get.getConf(CubeConf.QUERY_REWRITE_ENABLED) =>
        new GenPlanFromCache(spark).apply(transformPreCountDistinct(spark, plan))
      case _ => plan
    }
  }

  private def isCacheExists(
                             spark: SparkSession,
                             viewName: String,
                             formatInfo: CacheFormatInfo): Boolean = {
    val viewId = parseTableName(spark, viewName)
    cubeCatalog(spark).getCacheInfo(viewId) match {
      case Some(cacheInfo) =>
        formatInfo.cacheSchema match {
          case _: CacheRawSchema =>
            cacheInfo.rawCacheInfo.isDefined
          case _: CacheCubeSchema =>
            cacheInfo.cubeCacheInfo.isDefined
          case _ =>
            false
        }
      case _ =>
        false
    }
  }

  private def doCacheBuild(
                            session: SparkSession,
                            cacheId: CacheIdentifier,
                            buildInfo: Option[BuildInfo],
                            mode: SaveMode): Unit = {
    val startTime = System.currentTimeMillis
    try {
      getCacheInfo(session, cacheId) match {
        case Some(cacheInfo) =>
          val viewId = TableIdentifier(cacheId.viewName, cacheId.db)
          val dataSet = buildInfo match {
            case Some(build) =>
              SparkAgent.getDataFrame(session, viewId).filter(build.expr)
            case _ =>
              SparkAgent.getDataFrame(session, viewId)
          }

          if (buildingCaches.add(cacheId)) {
            try {
              // Disable query rewrite for cache at first, as it should not be used to rewrite
              // this build job.
              if (cacheInfo.getEnableRewrite) {
                alterCacheRewrite(session, cacheId, false)
              }
              writeCacheData(dataSet, viewId, cacheInfo, mode)

              // add build history info.
              val endTime = System.currentTimeMillis
              val buildRecord = BuildHistory(startTime, endTime, buildInfo, mode, "Succeed", "")
              val buildHistories = buildCacheHistories
                .getOrDefault(cacheId, Seq.empty[BuildHistory])
              buildCacheHistories.put(cacheId, buildHistories :+ buildRecord)
              // update last update time.
              alterCachePropertyInExternalCatalog(session, cacheId, (cacheInfo) => cacheInfo match {
                case rawCacheInfo: RawCacheInfo =>
                  rawCacheInfo.copy(lastUpdateTime = endTime)
                case cubeCacheInfo: CubeCacheInfo =>
                  cubeCacheInfo.copy(lastUpdateTime = endTime)
              })
              logInfo(s"Successfully build cache for ${cacheId.toString}," +
                s" filter[${buildInfo.map(_.toString)}], save mode[$mode]")
            } finally {
              buildingCaches.remove(cacheId)
              if (cacheInfo.getEnableRewrite) {
                alterCacheRewrite(session, cacheId, true)
              }
            }
          } else {
            throw new CacheIsBuildingException(cacheId)
          }
        case _ =>
          throw new CacheNotExistException(cacheId)
      }
    } catch {
      case t: Throwable =>
        logWarning(s"Failed to build cache for ${cacheId.toString} with" +
          s" ${buildInfo.map(_.toString)} in $mode mode.", t)
        val buildRecord = BuildHistory(startTime, -1, buildInfo, mode, "Failed", t.getMessage)
        val buildHistories = buildCacheHistories
          .getOrDefault(cacheId, Seq.empty[BuildHistory])
        buildCacheHistories.put(cacheId, buildHistories :+ buildRecord)
    }
  }

  private def writeCacheData(
                              viewData: Dataset[_],
                              viewName: TableIdentifier,
                              cacheInfo: BasicCacheInfo,
                              saveMode: SaveMode): Unit = {
    val spark = viewData.sparkSession
    val cacheSchema = cacheInfo.getCacheSchema
    val storageInfo = cacheInfo.getStorageInfo
    val cacheData = cacheSchema match {
      case CacheRawSchema(cols) =>
        viewData.select(cols.map(col): _*)
      case CacheCubeSchema(dims, measures) =>
        measures.map(toMeasureColumn).toList match {
          case head :: Nil =>
            viewData.groupBy(dims.map(col): _*).agg(head)
          case head :: other =>
            viewData.groupBy(dims.map(col): _*).agg(head, other: _*)
          case Nil =>
            throw new UnsupportedOperationException("not support empty agg list")
        }
    }
    // If pre_count_distinct exists, try to build global dictionary and update plan to use
    // dictionary directly. Global dictionary could be used for multi cache building.
    val withDictPlan = buildGlobalDictionary(spark, viewName, SparkAgent.getLogicalPlan(cacheData))
    logInfo(s"Build cache for ${cacheInfo.getCacheName} with plan:\n$withDictPlan")
    // TODO use zorderPlan instead of withDictPlan

    var dataRow = SparkAgent.getDataFrame(spark, withDictPlan)
    if (spark.sparkContext.getConf.getBoolean("spark.sql.cache.cacheByPartition", false)) {
      if (storageInfo.partitionSpec.isDefined) {
        val partitionList = storageInfo.partitionSpec.get.map(par => col(par))
        dataRow = dataRow.repartition(partitionList: _*)
      }
    }
    val finalPlan = GenPlanFromCache(dataRow.sparkSession).apply(dataRow.queryExecution.analyzed)

    logInfo(s"Build cache for ${cacheInfo.getCacheName} with final plan:\n$finalPlan")
    val newDataRow = SparkAgent.getDataFrame(dataRow.sparkSession, finalPlan)
    val cur = newDataRow
      .write
      .format(storageInfo.provider)
      .mode(saveMode)

    val partitioned = storageInfo.partitionSpec.map(cur.partitionBy).getOrElse(cur)
    val bucketed = storageInfo.bucketSpec.map {
      bucketSpec =>
        val bucketPartial = partitioned.bucketBy(bucketSpec.numBuckets,
          bucketSpec.bucketColumnNames.head, bucketSpec.bucketColumnNames.tail: _*)
        if (bucketSpec.sortColumnNames.isEmpty) {
          bucketPartial
        } else {
          bucketPartial.sortBy(bucketSpec.sortColumnNames.head, bucketSpec.sortColumnNames.tail: _*)
        }
    }.getOrElse(partitioned)
    bucketed.save(storageInfo.storagePath)

    // build file index
    val tmpViewName = UUID.randomUUID().toString.replace("-", "")
    if (SQLConf.get.getConf(CubeConf.DATA_SKIP_ENABLED)) {
      // TODO build data skip index
    }
    spark.catalog.dropTempView(tmpViewName)
  }

  private def toMeasureColumn(measure: Measure): Column = {
    measure.func match {
      case "SUM" | "Sum" | "sum" =>
        SparkAgent.createColumn(
          toAlias(Sum(col(measure.column).expr).toAggregateExpression, measure))
      case "COUNT" | "Count" | "count" =>
        SparkAgent.createColumn(
          toAlias(Count(col(measure.column).expr).toAggregateExpression, measure))
      case "AVG" | "Avg" | "avg" | "AVERAGE" | "Average" | "average" =>
        SparkAgent.createColumn(
          toAlias(Average(col(measure.column).expr).toAggregateExpression, measure))
      case "MIN" | "Min" | "min" =>
        SparkAgent.createColumn(
          toAlias(Min(col(measure.column).expr).toAggregateExpression, measure))
      case "MAX" | "Max" | "max" =>
        SparkAgent.createColumn(
          toAlias(Max(col(measure.column).expr).toAggregateExpression, measure))
      case "PRE_COUNT_DISTINCT" | "Pre_Count_Distinct" | "pre_count_distinct" =>
        SparkAgent.createColumn(
          toAlias(PreCountDistinct(col(measure.column).expr).toAggregateExpression, measure))
      case "PRE_APPROX_COUNT_DISTINCT" | "Pre_Approx_Count_Distinct" |
           "pre_approx_count_distinct" =>
        SparkAgent.createColumn(toAlias(
          PreApproxCountDistinct(col(measure.column).expr).toAggregateExpression, measure))
      case other: String =>
        throw SparkAgent.analysisException(s"Function $other is not support for pre calculation.")
    }
  }

  private def toAlias(expr: Expression, measure: Measure): Alias = {
    Alias(expr, measure.name)()
  }

  private def getCacheDefaultPath(session: SparkSession, dbName: String): Path = {
    new Path(new Path(session.sessionState.conf.warehousePath, dbName), "_CACHE")
  }

  private def addCacheMetaInExternalCatalog(
                                             session: SparkSession,
                                             tableIdentifier: TableIdentifier,
                                             cacheInfo: CacheFormatInfo,
                                             storageInfo: CacheStorageInfo): Unit = {
    val lastUpdateTime = System.currentTimeMillis()
    val oldCacheInfo = cubeCatalog(session).getCacheInfo(tableIdentifier)
    val updatedCacheInfo = cacheInfo.cacheSchema match {
      case rawSchema: CacheRawSchema =>
        val rawCacheInfo = RawCacheInfo(cacheInfo.cacheName, cacheInfo.rewriteEnabled,
          storageInfo, rawSchema, lastUpdateTime)
        if (oldCacheInfo.isEmpty) {
          CacheInfo(Some(rawCacheInfo), None)
        } else {
          oldCacheInfo.get.copy(rawCacheInfo = Some(rawCacheInfo))
        }
      case cubeSchema: CacheCubeSchema =>
        val cubeCacheInfo = CubeCacheInfo(cacheInfo.cacheName, cacheInfo.rewriteEnabled,
          storageInfo, cubeSchema, lastUpdateTime)
        if (oldCacheInfo.isEmpty) {
          CacheInfo(None, Some(cubeCacheInfo))
        } else {
          oldCacheInfo.get.copy(cubeCacheInfo = Some(cubeCacheInfo))
        }
    }
    // add cache information to cache table meta.
    cubeCatalog(session).setCacheInfo(tableIdentifier, updatedCacheInfo)
  }

  private def parseTableName(spark: SparkSession, tableName: String): TableIdentifier = {
    val tableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    tableIdentifier.database match {
      case Some(_) => tableIdentifier
      case None =>
        tableIdentifier.copy(database = None)
    }
  }

  private def alterCachePropertyInExternalCatalog(
                                                   spark: SparkSession,
                                                   cacheId: CacheIdentifier,
                                                   f: (BasicCacheInfo) => BasicCacheInfo): Unit = {
    val cacheInfoOpt = cubeCatalog(spark).getCacheInfo(cacheId)
    if (cacheInfoOpt.isEmpty || cacheInfoOpt.get.isEmpty) {
      throw new NotCachedException(cacheId.db.getOrElse(""), cacheId.viewName)
    }

    val cacheInfo = cacheInfoOpt.get
    val updatedCacheInfo =
      if (cacheInfo.rawCacheInfo.isDefined &&
        cacheInfo.rawCacheInfo.get.cacheName == cacheId.cacheName) {
        cacheInfo.copy(rawCacheInfo = cacheInfo.rawCacheInfo.map(f(_).asInstanceOf[RawCacheInfo]))
      } else if (cacheInfo.cubeCacheInfo.isDefined &&
        cacheInfo.cubeCacheInfo.get.cacheName == cacheId.cacheName) {
        cacheInfo
          .copy(cubeCacheInfo = cacheInfo.cubeCacheInfo.map(f(_).asInstanceOf[CubeCacheInfo]))
      } else {
        throw new CacheNotExistException(cacheId)
      }
    cubeCatalog(spark).setCacheInfo(cacheId, updatedCacheInfo)
  }

  private def deleteCacheData(
                               session: SparkSession,
                               storagePath: String,
                               partitionPath: Option[String],
                               ifExists: Boolean): Unit = {
    val path = partitionPath match {
      case Some(partPath) =>
        new Path(storagePath, partPath)
      case None => new Path(storagePath)
    }
    val fs = path.getFileSystem(session.sessionState.newHadoopConf)
    if (fs.exists(path)) {
      fs.delete(path, true)
    } else if (!ifExists) {
      throw SparkAgent.analysisException("$path does not exists, can not drop cache data.")
    }
  }

  private def buildGlobalDictionary(
                                     spark: SparkSession,
                                     cache: TableIdentifier,
                                     plan: LogicalPlan): LogicalPlan = transformPreCountDistinct(spark, plan) transform {
    case GlobalDictionaryPlaceHolder(expr: String, child: LogicalPlan) =>
      val dictPath = getDictionaryPath(spark, cache, expr)
      val fs = dictPath.getFileSystem(spark.sessionState.newHadoopConf)
      if (fs.exists(dictPath)) {
        val dict = spark.read.parquet(dictPath.toString)
        import spark.implicits._
        val maxOffset = dict.agg(max($"dict_value")).collect().head.getInt(0)
        val increPlan = child match {
          case Project(_, Project(_, Window(_, _, _, windowChild))) =>
            val windowSpec = org.apache.spark.sql.expressions.Window.orderBy(col(expr))
            SparkAgent.getLogicalPlan(SparkAgent.getDataFrame(spark, windowChild).join(
              dict,
              SparkAgent.createColumn(
                EqualTo(col(expr).expr, SparkAgent.getLogicalPlan(dict).output(0))),
              "left_anti").select(col(expr) as "dict_key",
              (row_number().over(windowSpec) + lit(maxOffset)) as "dict_value"))
          case _ => child
        }
        SparkAgent.getDataFrame(
          spark, increPlan).write.mode(SaveMode.Append).parquet(dictPath.toString)
        logInfo(s"Incremental persist global dictionary for $expr with LogicalPlan:" +
          s" \n${increPlan.toString}")
      } else {
        SparkAgent.getDataFrame(
          spark, child).write.mode(SaveMode.Overwrite).parquet(dictPath.toString)
        logInfo(s"Persist global dictionary for $expr with LogicalPlan:" +
          s" \n${child.toString}")
      }
      val dictPlan = SparkAgent.getLogicalPlan(spark.read.parquet(dictPath.toString))
      val key = dictPlan.output(0)
      val value = dictPlan.output(1)
      val existKey = child.output(0)
      val existValue = child.output(1)
      val keyAlias = Alias(key, existKey.name)(existKey.exprId)
      val valueAlias = Alias(value, existValue.name)(existValue.exprId)
      Project(Seq(keyAlias, valueAlias), dictPlan)
  }

  private def getDictionaryPath(
                                 session: SparkSession,
                                 cache: TableIdentifier,
                                 exprName: String): Path = {
    new Path(new Path(new Path(new Path(session.sessionState.conf.warehousePath,
      cache.database.get), "_DICTIONARY"), cache.table), exprName)
  }

  private def transformPreCountDistinct(session: SparkSession, plan: LogicalPlan): LogicalPlan = {
    val transformer = new PreCountDistinctTransformer(session)
    transformer.apply(plan)
  }

  private def verifyCachePlan(plan: LogicalPlan) = {
    plan.foreach {
      case lr: LocalRelation =>
        throw SparkAgent.analysisException(s"Does not support cache on LocalRelation($lr)")
      case _ =>
    }

    plan.transformAllExpressions {
      case nd: Expression if !nd.deterministic =>
        throw SparkAgent.analysisException(
          s"Should not cache with non-deterministic expression($nd)")
      case current@(_: CurrentDate | _: CurrentTimestamp | _: CurrentDatabase) =>
        throw SparkAgent.analysisException(
          s"Should not cache with ${current.prettyName} expression.")
    }
  }

  private def verifyCacheFormatInfo(
                                     cacheFormatInfo: CacheFormatInfo,
                                     schema: StructType): Unit = {
    assert(cacheFormatInfo.cacheName != null, "Cache name should not be null.")
    val validFieldNames = schema.fields.map(_.name)
    val validFieldStr = validFieldNames.mkString(", ")

    cacheFormatInfo.cacheSchema match {
      case rawSchema: CacheRawSchema =>
        val rawValidFieldNames = validFieldNames :+ "*"
        if (!rawSchema.cols.forall(rawValidFieldNames.contains)) {
          val rawColsStr = rawSchema.cols.mkString(", ")
          throw SparkAgent.analysisException(s"CacheRawSchema columns[${rawColsStr}]" +
            s" while valid field names are [$validFieldStr]")
        }
        if (cacheFormatInfo.partitionColumns.isDefined &&
          !cacheFormatInfo.partitionColumns.get.forall(rawValidFieldNames.contains)) {
          val partColStr = cacheFormatInfo.partitionColumns.get.mkString(", ")
          throw SparkAgent.analysisException(s"Partition columns[${partColStr}]" +
            s" while valid field names are [$validFieldStr]")
        }

        if (cacheFormatInfo.zorderColumns.isDefined &&
          !cacheFormatInfo.zorderColumns.get.forall(rawValidFieldNames.contains)) {
          val zorderColStr = cacheFormatInfo.zorderColumns.get.mkString(",")
          throw SparkAgent.analysisException(s"Zorder columns[${zorderColStr}]" +
            s" while valid field names are [$validFieldStr]")
        }
      case cubeSchema: CacheCubeSchema =>
        if (!cubeSchema.dims.forall(validFieldNames.contains)) {
          val cubeDimsStr = cubeSchema.dims.mkString(", ")
          throw SparkAgent.analysisException(s"CacheCubeSchema dimensions [${cubeDimsStr}]" +
            s" while valid field names are [$validFieldStr]")
        }
        if (!cubeSchema.measures.map(_.column).forall(validFieldNames.contains)) {
          val cubeMeasureColsStr = cubeSchema.measures.map(_.column).mkString(", ")
          throw SparkAgent.analysisException(s"CacheCubeSchema dimensions [${cubeMeasureColsStr}]" +
            s" while valid field names are [$validFieldStr]")
        }
        val dimsStr = cubeSchema.dims.mkString(", ")
        val measureStr = cubeSchema.measures.mkString(", ")
        if (cubeSchema.dims.intersect(cubeSchema.measures.map(_.column)).nonEmpty) {
          throw SparkAgent.analysisException(s"CacheCubeSchema dimensions should not intersect " +
            s"with measure columns, dims:[$dimsStr], measures:[$measureStr]")
        }
        if (cacheFormatInfo.partitionColumns.isDefined &&
          !cacheFormatInfo.partitionColumns.get.forall(cubeSchema.dims.contains)) {
          val partColStr = cacheFormatInfo.partitionColumns.get.mkString(", ")
          throw SparkAgent.analysisException(s"Partition columns[${partColStr}]" +
            s" while valid field names are [$dimsStr]")
        }
        if (cacheFormatInfo.zorderColumns.isDefined &&
          !cacheFormatInfo.zorderColumns.get.forall(cubeSchema.dims.contains)) {
          val zorderColStr = cacheFormatInfo.zorderColumns.get.mkString(",")
          throw SparkAgent.analysisException(s"Zorder columns[${zorderColStr}]" +
            s" while valid field names are [$dimsStr]")
        }
        cubeSchema.measures.map(toMeasureColumn)
    }

    if (cacheFormatInfo.partitionColumns.isDefined &&
      cacheFormatInfo.zorderColumns.isDefined &&
      cacheFormatInfo.partitionColumns.get
        .intersect(cacheFormatInfo.zorderColumns.get)
        .nonEmpty) {
      val partColStr = cacheFormatInfo.partitionColumns.get.mkString(", ")
      val zorderColStr = cacheFormatInfo.zorderColumns.get.mkString(",")
      throw SparkAgent.analysisException(s"Zorder columns[${zorderColStr}]" +
        s" should not intersect with partition columns [$partColStr]")
    }
  }

  /** Copied from InMemoryFileIndex. */
  def shouldFilterOut(pathName: String): Boolean = {
    // We filter follow paths:
    // 1. everything that starts with _ and ., except _common_metadata and _metadata
    // because Parquet needs to find those metadata files from leaf files returned by this method.
    // We should refactor this logic to not mix metadata files with data files.
    // 2. everything that ends with `._COPYING_`, because this is a intermediate state of file. we
    // should skip this file in case of double reading.
    val exclude = (pathName.startsWith("_") && !pathName.contains("=")) ||
      pathName.startsWith(".") || pathName.endsWith("._COPYING_")
    val include = pathName.startsWith("_common_metadata") || pathName.startsWith("_metadata")
    exclude && !include
  }

  private def clearRelatedInfo(cacheId: CacheIdentifier): Unit = {
    buildCacheHistories.remove(cacheId)
    cacheBuildTaskFutures.remove(cacheId)
    buildingCaches.remove(cacheId)
  }


  private def doAutoBuildCache(
                                session: SparkSession,
                                cacheId: CacheIdentifier,
                                periodBuildInfo: PeriodBuildInfo): Unit = {
    val task = new Runnable {
      override def run(): Unit = {
        periodBuildInfo.getSaveMode match {
          case SaveMode.Append =>
            buildCache(session, cacheId, periodBuildInfo.getNextBuildInfo)
          case SaveMode.Overwrite =>
            refreshCache(session, cacheId, periodBuildInfo.getNextBuildInfo)
          case _ =>
            throw new SparkCubeException(cacheId,
              s"update cache data only support Append/Overwrite mode," +
                s" ${periodBuildInfo.getSaveMode} is not supported.")
        }
      }
    }
    val delay = (periodBuildInfo.getTriggerTime - System.currentTimeMillis()) / 1000

    val future = cacheBuildExecutor
      .scheduleAtFixedRate(task, delay, periodBuildInfo.getPeriod, TimeUnit.SECONDS)
    logInfo(s"Schedule period automatic cache build for ${cacheId.toString} with period" +
      s" $periodBuildInfo")
    cacheBuildTaskFutures.put(cacheId, (periodBuildInfo, future))
  }

  private implicit def cacheIdToTableIdent(cacheIdentifier: CacheIdentifier): TableIdentifier = {
    TableIdentifier(cacheIdentifier.viewName, cacheIdentifier.db)
  }
}

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

import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SaveMode, SparkAgent, SparkSession}

import com.alibaba.sparkcube.CubeManager
import com.alibaba.sparkcube.catalog.{CubeCacheInfo, RawCacheInfo}
import com.alibaba.sparkcube.execution.{BuildHistory, PeriodBuildInfo}
import com.alibaba.sparkcube.optimizer.CacheIdentifier

/**
 * Due to the limitation of YARN proxy server and knox, v1 api only accept GET and PUT request, and
 * use application/json as content-type.
 */

@Path("/v1")
class SparkCubeSource extends CacheApiRequestContext {

  @GET
  @Path("caches")
  def applicationList(): Iterator[CacheBasicInfo] = {
    cacheManager.listAllCaches(sparkSession).flatMap {
      cache =>
        val tableId = cache._1
        val cacheInfo = cache._2
        val optionalRaw = cacheInfo.rawCacheInfo.map {
          rawCacheInfo =>
            CacheBasicInfo(tableId.database, tableId.table, rawCacheInfo.cacheName,
              rawCacheInfo.enableRewrite, getFileSize(rawCacheInfo.storageInfo.storagePath),
              SparkAgent.formatDate(rawCacheInfo.lastUpdateTime))
        }
        val optionalCube = cacheInfo.cubeCacheInfo.map {
          cubeCacheInfo =>
            CacheBasicInfo(tableId.database, tableId.table, cubeCacheInfo.cacheName,
              cubeCacheInfo.enableRewrite, getFileSize(cubeCacheInfo.storageInfo.storagePath),
              SparkAgent.formatDate(cubeCacheInfo.lastUpdateTime))
        }
        Seq(optionalRaw, optionalCube).flatten
    }.toIterator
  }

  @GET
  @Path("caches/{cacheId}")
  def cacheDetail(@PathParam("cacheId") cacheId: String): CacheDetailInfo = {
    val identifier = CacheIdentifier(cacheId)
    cacheManager.getCacheInfo(sparkSession, identifier) match {
      case Some(rawCacheInfo: RawCacheInfo) =>
        RawCacheDetailInfo(identifier.db, identifier.viewName, identifier.cacheName,
          rawCacheInfo.enableRewrite, getFileSize(rawCacheInfo.storageInfo.storagePath),
          SparkAgent.formatDate(rawCacheInfo.lastUpdateTime), "RAW", rawCacheInfo.cacheSchema.cols,
          rawCacheInfo.storageInfo.storagePath, rawCacheInfo.storageInfo.provider,
          rawCacheInfo.storageInfo.partitionSpec.getOrElse(Nil),
          rawCacheInfo.storageInfo.zorder.getOrElse(Nil))
      case Some(cubeCacheInfo: CubeCacheInfo) =>
        CubeCacheDetailInfo(identifier.db, identifier.viewName, identifier.cacheName,
          cubeCacheInfo.enableRewrite, getFileSize(cubeCacheInfo.storageInfo.storagePath),
          SparkAgent.formatDate(cubeCacheInfo.lastUpdateTime), "CUBE",
          cubeCacheInfo.cacheSchema.dims,
          cubeCacheInfo.cacheSchema.measures.map{measure => s"${measure.func}(${measure.column})"},
          cubeCacheInfo.storageInfo.storagePath, cubeCacheInfo.storageInfo.provider,
          cubeCacheInfo.storageInfo.partitionSpec.getOrElse(Nil),
          cubeCacheInfo.storageInfo.zorder.getOrElse(Nil))
      case _ => null
    }
  }

  @PUT
  @Path("caches/{viewName}")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def createCache(cacheInfo: StringParam,
                  @PathParam("viewName") viewName: String): ActionResponse = {
    try {
      val flag = cacheManager.createCache(sparkSession, viewName,
        JsonParserUtil.parseCacheFormatInfo(cacheInfo.getParam))
      if (flag) {
        ActionResponse("SUCCEED", "")
      } else {
        ActionResponse("FAILED", "")
      }
    } catch {
      case t: Throwable => ActionResponse("ERROR", t.getMessage)
    }
  }

  @PUT
  @Path("caches/{cacheId}/delete")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def dropCache(@PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.dropCache(sparkSession, identifier)
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @PUT
  @Path("caches/{cacheId}/enable")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def setQueryRewrite(enableParam: StringParam,
      @PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.alterCacheRewrite(sparkSession, identifier, enableParam.getParam.toBoolean)
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @GET
  @Path("caches/{cacheId}/partitions")
  def cacheDataPartitions(@PathParam("cacheId") cacheId: String): Iterator[CachePartitionInfo] = {
    val identifier = CacheIdentifier(cacheId)
    val cacheInfo = cacheManager.getCacheInfo(sparkSession, identifier).get
    cacheManager.listCachePartitions(sparkSession, identifier).map {
      path =>
        CachePartitionInfo(path, getFileSize(cacheInfo.getStorageInfo.storagePath + "/" + path))
    }.toIterator
  }

  @PUT
  @Path("caches/{cacheId}/partitions/delete")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def removeCacheDataPartition(deletePartition: StringParam,
      @PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.dropCachePartition(sparkSession, identifier, Seq(deletePartition.getParam))
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @PUT
  @Path("caches/{cacheId}/build")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def buildCache( buildInfo: StringParam,
      @PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      val (saveMode, build) = JsonParserUtil.parseBuildInfo(buildInfo.getParam)
      saveMode match {
        case SaveMode.Append =>
          cacheManager.asyncBuildCache(sparkSession, identifier, build)
        case SaveMode.Overwrite =>
          cacheManager.asyncRefreshCache(sparkSession, identifier, build)
        case _ =>
          throw new UnsupportedOperationException("not supported save mode")
      }
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @GET
  @Path("caches/{cacheId}/build")
  def isCacheBuildingNow(@PathParam("cacheId") cacheId: String): Boolean = {
    val identifier = CacheIdentifier(cacheId)
    cacheManager.isCacheUnderBuilding(identifier)
  }

  @PUT
  @Path("caches/{cacheId}/periodBuild")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def triggerPeriodBuildCache(periodBuildParam: StringParam,
      @PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      val periodBuildInfo = JsonParserUtil.parsePeriodBuildInfo(periodBuildParam.getParam)
      cacheManager.autoBuildCache(sparkSession, identifier, periodBuildInfo)
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @PUT
  @Path("caches/{cacheId}/periodBuild/delete")
  def cancelPeriodBuildCache(@PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.cancelAutoBuildCache(identifier)
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @GET
  @Path("caches/{cacheId}/buildHistory")
  def getBuildHistory(@PathParam("cacheId") cacheId: String): Iterator[BuildHistory] = {
    val identifier = CacheIdentifier(cacheId)
    cacheManager.listBuildHistory(sparkSession, identifier).toIterator
  }

  @PUT
  @Path("caches/{cacheId}/buildHistory/delete")
  def clearBuildHistory(@PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.clearBuildHistory(Some(identifier))
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }
}

@Path("/v2")
class SparkCubeSourceV2 extends CacheApiRequestContext {

  @GET
  @Path("caches")
  def applicationList(): Iterator[CacheBasicInfo] = {
    cacheManager.listAllCaches(sparkSession).flatMap {
      cache =>
        val tableId = cache._1
        val cacheInfo = cache._2
        val optionalRaw = cacheInfo.rawCacheInfo.map {
          rawCacheInfo =>
            CacheBasicInfo(tableId.database, tableId.table, rawCacheInfo.cacheName,
              rawCacheInfo.enableRewrite, getFileSize(rawCacheInfo.storageInfo.storagePath),
              SparkAgent.formatDate(rawCacheInfo.lastUpdateTime))
        }
        val optionalCube = cacheInfo.cubeCacheInfo.map {
          cubeCacheInfo =>
            CacheBasicInfo(tableId.database, tableId.table, cubeCacheInfo.cacheName,
              cubeCacheInfo.enableRewrite, getFileSize(cubeCacheInfo.storageInfo.storagePath),
              SparkAgent.formatDate(cubeCacheInfo.lastUpdateTime))
        }
        Seq(optionalRaw, optionalCube).flatten
    }.toIterator
  }

  @GET
  @Path("caches/{cacheId}")
  def cacheDetail(@PathParam("cacheId") cacheId: String): CacheDetailInfo = {
    val identifier = CacheIdentifier(cacheId)
    cacheManager.getCacheInfo(sparkSession, identifier) match {
      case Some(rawCacheInfo: RawCacheInfo) =>
        RawCacheDetailInfo(identifier.db, identifier.viewName, identifier.cacheName,
          rawCacheInfo.enableRewrite, getFileSize(rawCacheInfo.storageInfo.storagePath),
          SparkAgent.formatDate(rawCacheInfo.lastUpdateTime), "RAW", rawCacheInfo.cacheSchema.cols,
          rawCacheInfo.storageInfo.storagePath, rawCacheInfo.storageInfo.provider,
          rawCacheInfo.storageInfo.partitionSpec.getOrElse(Nil),
          rawCacheInfo.storageInfo.zorder.getOrElse(Nil))
      case Some(cubeCacheInfo: CubeCacheInfo) =>
        CubeCacheDetailInfo(identifier.db, identifier.viewName, identifier.cacheName,
          cubeCacheInfo.enableRewrite, getFileSize(cubeCacheInfo.storageInfo.storagePath),
          SparkAgent.formatDate(cubeCacheInfo.lastUpdateTime), "CUBE",
          cubeCacheInfo.cacheSchema.dims,
          cubeCacheInfo.cacheSchema.measures.map{measure => s"${measure.func}(${measure.column})"},
          cubeCacheInfo.storageInfo.storagePath, cubeCacheInfo.storageInfo.provider,
          cubeCacheInfo.storageInfo.partitionSpec.getOrElse(Nil),
          cubeCacheInfo.storageInfo.zorder.getOrElse(Nil))
      case _ => null
    }
  }

  @POST
  @Path("caches/{viewName}")
  @Consumes(Array(MediaType.APPLICATION_FORM_URLENCODED))
  def createCache(
    @PathParam("viewName") viewName: String,
    @FormParam("cacheInfo") cacheInfo: String): ActionResponse = {
    try {
      val flag = cacheManager.createCache(sparkSession, viewName,
        JsonParserUtil.parseCacheFormatInfo(cacheInfo))
      ActionResponse("SUCCEED", "")
      if (flag) {
        ActionResponse("SUCCEED", "")
      } else {
        ActionResponse("FAILED", "")
      }
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
      case t: Throwable => ActionResponse("ERROR", t.getMessage)
    }
  }

  @DELETE
  @Path("caches/{cacheId}")
  def dropCache(@PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.dropCache(sparkSession, identifier)
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @PUT
  @Path("caches/{cacheId}/enable")
  @Consumes(Array(MediaType.APPLICATION_FORM_URLENCODED))
  def setQueryRewrite(
    @PathParam("cacheId") cacheId: String,
    @FormParam("enable") enable: Boolean): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.alterCacheRewrite(sparkSession, identifier, enable)
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @GET
  @Path("caches/{cacheId}/partitions")
  def cacheDataPartitions(@PathParam("cacheId") cacheId: String): Iterator[CachePartitionInfo] = {
    val identifier = CacheIdentifier(cacheId)
    val cacheInfo = cacheManager.getCacheInfo(sparkSession, identifier).get
    cacheManager.listCachePartitions(sparkSession, identifier).map {
      path =>
        CachePartitionInfo(path, getFileSize(cacheInfo.getStorageInfo.storagePath + "/" + path))
    }.toIterator
  }

  @DELETE
  @Path("caches/{cacheId}/partitions")
  @Consumes(Array(MediaType.APPLICATION_FORM_URLENCODED))
  def removeCacheDataPartition(
    @PathParam("cacheId") cacheId: String,
    @FormParam("partitionPath") partitionPath: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.dropCachePartition(sparkSession, identifier, Seq(partitionPath))
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @POST
  @Path("caches/{cacheId}/build")
  @Consumes(Array(MediaType.APPLICATION_FORM_URLENCODED))
  def buildCache(
    @PathParam("cacheId") cacheId: String,
    @FormParam("buildInfo") buildInfo: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      val (saveMode, build) = JsonParserUtil.parseBuildInfo(buildInfo)
      saveMode match {
        case SaveMode.Append =>
          cacheManager.asyncBuildCache(sparkSession, identifier, build)
        case SaveMode.Overwrite =>
          cacheManager.asyncRefreshCache(sparkSession, identifier, build)
        case _ =>
          throw new UnsupportedOperationException("not supported save mode")
      }
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @GET
  @Path("caches/{cacheId}/build/status")
  def isCacheBuildingNow(@PathParam("cacheId") cacheId: String): Boolean = {
    val identifier = CacheIdentifier(cacheId)
    cacheManager.isCacheUnderBuilding(identifier)
  }

  @GET
  @Path("caches/{cacheId}/timeTriggerBuild")
  def getTriggerPeriodBuildInfo(@PathParam("cacheId") cacheId: String): Option[PeriodBuildInfo] = {
    val identifier = CacheIdentifier(cacheId)
    cacheManager.getAutoBuildCache(identifier)
  }

  @POST
  @Path("caches/{cacheId}/timeTriggerBuild")
  @Consumes(Array(MediaType.APPLICATION_FORM_URLENCODED))
  def triggerPeriodBuildCache(
    @PathParam("cacheId") cacheId: String,
    @FormParam("timeTriggerBuildInfo") periodBuildJson: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      val periodBuildInfo = JsonParserUtil.parsePeriodBuildInfo(periodBuildJson)
      cacheManager.autoBuildCache(sparkSession, identifier, periodBuildInfo)
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @DELETE
  @Path("caches/{cacheId}/timeTriggerBuild")
  def cancelPeriodBuildCache(@PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.cancelAutoBuildCache(identifier)
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }

  @GET
  @Path("caches/{cacheId}/buildHistory")
  def getBuildHistory(@PathParam("cacheId") cacheId: String): Iterator[BuildHistory] = {
    val identifier = CacheIdentifier(cacheId)
    cacheManager.listBuildHistory(sparkSession, identifier).toIterator
  }

  @DELETE
  @Path("caches/{cacheId}/buildHistory")
  def clearBuildHistory(@PathParam("cacheId") cacheId: String): ActionResponse = {
    try {
      val identifier = CacheIdentifier(cacheId)
      cacheManager.clearBuildHistory(Some(identifier))
      ActionResponse("SUCCEED", "")
    } catch {
      case t: Throwable => ActionResponse("FAILED", t.getMessage)
    }
  }
}

object SparkCubeSource {

}

private[api] trait CacheApiRequestContext {

  val sparkSession = SparkSession.builder().getOrCreate()
  val conf = new Configuration()

  def cacheManager: CubeManager = null

  def getFileSize(path: String): String = {
    val cachePath = new org.apache.hadoop.fs.Path(path)
    val fs = cachePath.getFileSystem(conf)
    if (fs.exists(cachePath)) {
      val fileSize = fs.getContentSummary(cachePath).getLength()
      if (fileSize < 1024) {
        fileSize + " B"
      } else if (fileSize < 1024 * 1024) {
        fileSize / 1024 + " KB"
      } else if (fileSize < 1024 * 1024 * 1024) {
        fileSize / (1024 * 1024) + " MB"
      } else {
        fileSize / (1024 * 1024 * 1024) + " GB"
      }
    } else {
      "None"
    }
  }
}

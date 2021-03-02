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

import java.io.OutputStream
import java.lang.annotation.Annotation
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, SimpleTimeZone}
import java.util.concurrent.TimeUnit
import javax.ws.rs.Produces
import javax.ws.rs.core.{MediaType, MultivaluedMap}
import javax.ws.rs.ext.{MessageBodyWriter, Provider}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import org.joda.time.format.DateTimeFormat
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkAgent}

import com.alibaba.sparkcube.execution._
import com.alibaba.sparkcube.optimizer._

/**
 * This class converts the POJO metric responses into json, using jackson.
 *
 * This doesn't follow the standard jersey-jackson plugin options, because we want to stick
 * with an old version of jersey (since we have it from yarn anyway) and don't want to pull in lots
 * of dependencies from a new plugin.
 *
 * Note that jersey automatically discovers this class based on its package and its annotations.
 */
@Provider
@Produces(Array(MediaType.APPLICATION_JSON))
private[api] class JacksonMessageWriter extends MessageBodyWriter[Object]{

  val mapper = new ObjectMapper() {
    override def writeValueAsString(t: Any): String = {
      super.writeValueAsString(t)
    }
  }
  mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat)

  override def isWriteable(
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType): Boolean = {
      true
  }

  override def writeTo(
      t: Object,
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType,
      multivaluedMap: MultivaluedMap[String, AnyRef],
      outputStream: OutputStream): Unit = {
    mapper.writeValue(outputStream, t)
  }

  override def getSize(
      t: Object,
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType): Long = {
    -1L
  }
}

private[api] object JacksonMessageWriter {
  def makeISODateFormat: SimpleDateFormat = {
    val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'", Locale.US)
    val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
    iso8601.setCalendar(cal)
    iso8601
  }
}

object JsonParserUtil extends Logging{
  implicit val formats = DefaultFormats

  def parseBuildInfo(json: String): (SaveMode, Option[BuildInfo]) = {
    fromBuildJsonValue(parse(json))
  }

  def parsePeriodBuildInfo(json: String): PeriodBuildInfo = {
    fromPeriodJsonValue(parse(json))
  }

  def parseCacheFormatInfo(json: String): CacheFormatInfo = {
    fromCacheFormatInfo(parse(json))
  }

  private def fromBuildJsonValue(value: JValue): (SaveMode, Option[BuildInfo]) = value match {
    case JSortedObject(
    ("data", data),
    ("saveMode", JString(saveMode)),
    ("type", JString(infoType))) =>
      infoType match {
        case "range" =>
          data match {
            case JSortedObject(
            ("columnName", JString(columnName)),
            ("dateFormat", JString(dateFormat)),
            ("end", JString(end)),
            ("start", JString(start))) =>
              val endOpt = if (end == null || end.isEmpty) None else Some(end)
              val dateFormatOpt =
                if (dateFormat == null || dateFormat.isEmpty) None else Some(dateFormat)
              (SaveMode.valueOf(saveMode),
                Some(RangeValueBuildInfo(columnName, start, endOpt, dateFormatOpt)))
            case _ =>
              throw SparkAgent.analysisException(s"Invalid Build Info: ${compact(render(value))}")
          }
        case "fixed" =>
          data match {
            case JSortedObject(
            ("columnName", JString(columnName)),
            ("values", JString(values))) =>
              val colValues = values.split(",")
              (SaveMode.valueOf(saveMode), Some(FixedValueBuildInfo(columnName, colValues)))
            case _ =>
              throw SparkAgent.analysisException(s"Invalid Build Info: ${compact(render(value))}")
          }
        case "full" =>
          (SaveMode.valueOf(saveMode), None)
        case _ =>
          throw SparkAgent.analysisException(s"Invalid Build Info: ${compact(render(value))}")
      }
  }

  private def fromPeriodJsonValue(value: JValue): PeriodBuildInfo = value match {
    case JSortedObject(("data", data), ("type", JString(infoType))) =>
      infoType match {
        case "full" =>
          data match {
            case JSortedObject(
            ("period", JString(period)),
            ("periodTimeUnit", JString(timeUnit)),
            ("saveMode", JString(saveMode)),
            ("triggerTime", JString(buildTriggerTime))) =>
              FullPeriodUpdateInfo(getTriggerTime(buildTriggerTime), period.toInt,
                TimeUnit.valueOf(timeUnit), SaveMode.valueOf(saveMode))
            case _ =>
              throw SparkAgent.analysisException(s"Invalid Build Info: ${compact(render(value))}")
          }
        case "timestamp" =>
          data match {
            case JSortedObject(
            ("colName", JString(colName)),
            ("period", JString(period)),
            ("periodTimeUnit", JString(timeUnit)),
            ("saveMode", JString(saveMode)),
            ("triggerTime", JString(triggerTime))) =>
              TimeStampPeriodUpdateInfo(colName, period.toInt, TimeUnit.valueOf(timeUnit),
                getTriggerTime(triggerTime), SaveMode.valueOf(saveMode))
            case _ =>
              throw SparkAgent.analysisException(s"Invalid Build Info: ${compact(render(value))}")
          }
        case "datetime" =>
          data match {
            case JSortedObject(
            ("colName", JString(colName)),
            ("dateFormat", JString(dateFormat)),
            ("period", JString(period)),
            ("periodTimeUnit", JString(timeUnit)),
            ("saveMode", JString(saveMode)),
            ("triggerTime", JString(triggerTime))) =>
              StringDatePeriodBuildInfo(colName, dateFormat, period.toInt,
                TimeUnit.valueOf(timeUnit), getTriggerTime(triggerTime), SaveMode.valueOf(saveMode))
          }
        case _ =>
          throw SparkAgent.analysisException(s"Invalid Build Info: ${compact(render(value))}")
      }
  }

  def getTriggerTime(timeStr: String): Long = {
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(timeStr).getMillis
  }

  private def fromCacheFormatInfo(value: JValue): CacheFormatInfo = value match {
    case JSortedObject(
    ("cacheName", JString(cacheName)),
    ("partitionColumns", JArray(partitionColumns)),
    ("provider", JString(provider)),
    ("rewriteEnabled", JString(rewriteEnabled)),
    ("schema", schema),
    ("type", JString(cacheType)),
    ("zorderColumns", JArray(zorderColumns))
    ) =>
      var enableRewrite: Boolean = true
      if ("DISABLE".equals(rewriteEnabled)) {
        enableRewrite = false
      }
      cacheType match {
        case "raw" =>
          schema match {
            case JSortedObject(
            ("selectColumns", JArray(selectColumns))
            ) =>
              val cacheRawSchema = CacheRawSchema(selectColumns.map(_.extract[String]))
              val partCols = if (partitionColumns.isEmpty) {
                None
              } else {
                Some(partitionColumns.map(_.extract[String]))
              }
              val zorderCols = if (zorderColumns.isEmpty) {
                None
              } else {
                Some(zorderColumns.map(_.extract[String]))
              }
              CacheFormatInfo(cacheName, enableRewrite, partCols, zorderCols, provider, None,
                cacheRawSchema)
            case _ =>
              throw SparkAgent.analysisException(s"Invalid create cache request parameter." +
                s" ${compact(render(value))}")
          }
        case "cube" =>
          schema match {
            case JSortedObject(
            ("dimensions", JArray(dims)),
            ("measuresFiled", JArray(measuresFiled)),
            ("measuresFunction", JArray(measuresFunction))
            ) =>
              val mField = measuresFiled.map(_.extract[String])
              val mFunc = measuresFunction.map(_.extract[String])
              val measure = mField.zip(mFunc).map(line => Measure(line._1, line._2))
              val cacheCubeSchema = CacheCubeSchema(dims.map(_.extract[String]), measure)
              val partCols = if (partitionColumns.isEmpty) {
                None
              } else {
                Some(partitionColumns.map(_.extract[String]))
              }
              val zorderCols = if (zorderColumns.isEmpty) {
                None
              } else {
                Some(zorderColumns.map(_.extract[String]))
              }
              CacheFormatInfo(cacheName, enableRewrite, partCols, zorderCols, provider, None,
                cacheCubeSchema)
            case _ =>
              throw SparkAgent.analysisException(s"Invalid create cache request parameter." +
                s" ${compact(render(value))}")
          }
        case _ =>
          throw SparkAgent.analysisException(s"Invalid create cache request parameter." +
            s" ${compact(render(value))}")
      }
  }
}

object JSortedObject {
  def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
    case JObject(seq) => Some(seq.sortBy(_._1))
    case _ => None
  }
}


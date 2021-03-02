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

package com.alibaba.sparkcube.execution

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.{Column, SaveMode, SparkAgent}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.sql.types.{LongType, StringType, StructType}

import com.alibaba.sparkcube.optimizer.CacheIdentifier


case class CacheRepresentation(_logicalRelation: LogicalPlan) {

  def relation: LogicalPlan = _logicalRelation

  def withTargetOutput(targetOutput: Seq[Attribute]): LogicalPlan =
    _logicalRelation match {
      case lr: LogicalRelation =>
        LogicalRelation(lr.relation,
          lr.output.map(replaceIfAny(_, targetOutput)),
          lr.catalogTable, lr.isStreaming)
      case hr: HiveTableRelation =>
        HiveTableRelation(
          hr.tableMeta,
          hr.output.map(replaceIfAny(_, targetOutput)),
          hr.partitionCols)
      case _ => throw SparkAgent.analysisException("Unsupported catalog relation")
    }

  private def replaceIfAny(
      ar: AttributeReference, requiredOutput: Seq[Attribute]): AttributeReference = {
    requiredOutput.find(o => ar.name.equals(o.name)) match {
      case Some(matched) =>
        ar.withExprId(matched.exprId)
      case None =>
        ar
    }
  }
}

trait BuildInfo {

  def expr: Column

  def verify(cacheId: CacheIdentifier, schema: StructType): Unit

}

/**
 * Extra information to build cache, with column between [start, end)
 */
case class RangeValueBuildInfo(
    columnName: String,
    start: String,
    endOpt: Option[String] = None,
    dateTimeFormat: Option[String] = None) extends BuildInfo {
  override def expr: Column = endOpt match {
    case Some(end) =>
      dateTimeFormat match {
        case Some(format) =>
          val greatOrEqualThanStart = GreaterThanOrEqual(
            to_timestamp(col(columnName), format).expr,
            to_timestamp(lit(start), format).expr)
          val lessThanEnd = LessThan(to_timestamp(col(columnName), format).expr,
            to_timestamp(lit(end), format).expr)
          SparkAgent.createColumn(And(greatOrEqualThanStart, lessThanEnd))
        case None =>
          SparkAgent.createColumn(And(GreaterThanOrEqual(col(columnName).expr, lit(start).expr),
            LessThan(col(columnName).expr, lit(end).expr)))
      }
    case _ =>
      dateTimeFormat match {
        case Some(format) =>
          SparkAgent.createColumn(GreaterThanOrEqual(
            to_timestamp(col(columnName), format).expr,
            to_timestamp(lit(start), format).expr))
        case None =>
          SparkAgent.createColumn(GreaterThanOrEqual(col(columnName).expr, lit(start).expr))
      }
  }

  override def toString: String = {
    val str = endOpt match {
      case Some(end) =>
        s"$columnName >= $start AND $columnName < $end"
      case _ =>
        s"$columnName >= $start"
    }
    s"($str)"
  }

  override def verify(cacheId: CacheIdentifier, schema: StructType): Unit = {
    if (!schema.fields.map(_.name).contains(columnName)) {
      throw new SparkCubeException(cacheId, s"Invalid build request, column[$columnName]" +
        s" does not exists in view fields[${schema.fields.map(_.name).mkString(",")}]")
    }

    if (dateTimeFormat.isDefined) {
      schema.fields.map(field => (field.name -> field.dataType)).toMap.get(columnName) match {
        case Some(StringType) =>
        case _ =>
          throw new SparkCubeException(cacheId, s"Invalid build request, you set" +
            s" DateTimeFormat for column[$columnName] while it's not String type.")
      }
    }
  }
}

case class FixedValueBuildInfo(columnName: String, values: Seq[String]) extends BuildInfo {
  override def expr: Column = {
    values.toList match {
      case head :: Nil =>
        SparkAgent.createColumn(EqualTo(col(columnName).expr, lit(head).expr))
      case head :: tail =>
        val headExpr: Expression = EqualTo(col(columnName).expr, lit(head).expr)
        val result = tail.foldLeft(headExpr) { (z, value) =>
          Or(z, EqualTo(col(columnName).expr, lit(value).expr))
        }
        SparkAgent.createColumn(result)
      case Nil => throw new UnsupportedOperationException("values should not be empty")
    }
  }

  override def toString: String = {
    val str = values.map(value => s"$columnName = '$value'").mkString(" OR ")
    s"($str)"
  }

  override def verify(cacheId: CacheIdentifier, schema: StructType): Unit = {
    if (!schema.fields.map(_.name).contains(columnName)) {
      throw new SparkCubeException(cacheId, s"Invalid build request, column[$columnName]" +
        s" does not exists in view fields[${schema.fields.map(_.name).mkString(",")}]")
    }
  }
}

case class MultiColumnBuildInfo(columns: Seq[BuildInfo]) extends BuildInfo {
  override def expr: Column = columns match {
    case head :: Nil =>
      head.expr
    case head :: tail =>
      val result = tail.foldLeft(head.expr.expr) {
        (z, value) =>
          And(z, value.expr.expr)
      }
      SparkAgent.createColumn(result)
  }

  override def toString(): String = {
    s"(${columns.map(_.toString).mkString(" AND ")})"
  }

  override def verify(cacheId: CacheIdentifier, schema: StructType): Unit = {
    columns.foreach(_.verify(cacheId, schema))
  }
}

trait PeriodBuildInfo {
  def getNextBuildInfo: Option[BuildInfo]

  def getTriggerTime: Long

  def getPeriod: Long

  def getSaveMode: SaveMode

  def verify(cacheId: CacheIdentifier, schema: StructType): Unit
}

case class StringDatePeriodBuildInfo(
    columnName: String,
    columnDateFormat: String,
    period: Int,
    periodUnit: TimeUnit,
    triggerTime: Long,
    saveMode: SaveMode) extends PeriodBuildInfo {
  var startTime = triggerTime - getPeriod * 1000

  override def getNextBuildInfo: Option[BuildInfo] = {
    val endTime = startTime + getPeriod * 1000
    val startValue = DateTimeFormat.forPattern(columnDateFormat).print(startTime)
    val endValue = DateTimeFormat.forPattern(columnDateFormat).print(endTime)
    startTime = endTime
    Some(RangeValueBuildInfo(columnName, startValue, Some(endValue), Some(columnDateFormat)))
  }

  override def getTriggerTime: Long = triggerTime

  override def getPeriod: Long = periodUnit match {
    case HOURS =>
      period * 60 * 60
    case MINUTES =>
      period * 60
    case SECONDS =>
      period
    case _ =>
      throw SparkAgent.analysisException(s"Only support HOURS/MINUTES/SECONDS time unit.")
  }

  override def getSaveMode: SaveMode = saveMode

  override def toString(): String = {
    s"Trigger at ${DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(triggerTime)}," +
      s" with period $period $periodUnit, step by column $columnName as datetime in" +
      s" format $columnDateFormat, $saveMode to exits cache data."
  }

  override def verify(cacheId: CacheIdentifier, schema: StructType): Unit = {
    require(columnDateFormat != null && columnDateFormat.nonEmpty,
      "DateTimeFormat should not be null or empty.")
    if (!schema.fields.map(_.name).contains(columnName)) {
      throw new SparkCubeException(cacheId, s"Invalid build request, column[$columnName]" +
      s" does not exists in view fields[${schema.fields.map(_.name).mkString(",")}]")
    }
    schema.fields.map(field => (field.name -> field.dataType)).toMap.get(columnName) match {
      case Some(StringType) =>
      case _ =>
        throw new SparkCubeException(cacheId, s"Invalid build request, you set" +
          s" DateTimeFormat for column[$columnName] while it's not String type.")
    }
  }
}

case class FullPeriodUpdateInfo(
    triggerTime: Long,
    period: Int,
    periodTimeUnit: TimeUnit,
    saveMode: SaveMode) extends PeriodBuildInfo {

  override def getNextBuildInfo: Option[BuildInfo] = None

  override def getTriggerTime: Long = triggerTime

  override def getPeriod: Long = periodTimeUnit match {
    case DAYS =>
      period * 24 * 60 * 60
    case HOURS =>
      period * 60 * 60
    case MINUTES =>
      period * 60
    case SECONDS =>
      period
    case _ =>
      throw SparkAgent.analysisException(s"Only support HOURS/MINUTES/SECONDS time unit.")
  }

  override def getSaveMode: SaveMode = saveMode

  override def toString(): String = {
    s"Trigger at ${DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(triggerTime)}," +
      s" with period $period $periodTimeUnit, $saveMode to exits cache data."
  }

  override def verify(cacheId: CacheIdentifier, schema: StructType): Unit = {}
}

case class TimeStampPeriodUpdateInfo(
    columnName: String,
    period: Int,
    periodTimeUnit: TimeUnit,
    triggerTime: Long,
    saveMode: SaveMode) extends PeriodBuildInfo {
  var startOffset = triggerTime - getPeriod * 1000

  override def getNextBuildInfo: Option[BuildInfo] = {
    val current = System.currentTimeMillis
    val buildInfo = RangeValueBuildInfo(columnName, startOffset.toString, Some(current.toString))
    startOffset = current
    Some(buildInfo)
  }

  override def getTriggerTime: Long = triggerTime

  override def getPeriod: Long = periodTimeUnit match {
    case HOURS =>
      period * 60 * 60
    case MINUTES =>
      period * 60
    case SECONDS =>
      period
    case _ =>
      throw SparkAgent.analysisException(s"Only support HOURS/MINUTES/SECONDS time unit.")
  }

  override def getSaveMode: SaveMode = saveMode

  override def toString(): String = {
    s"Trigger at ${DateTimeFormat.forPattern("yyyy-MM-dd HH-mm-ss").print(triggerTime)}," +
      s" with period $period $periodTimeUnit, step by column $columnName as timestamp," +
      s" $saveMode to exits cache data."
  }

  override def verify(cacheId: CacheIdentifier, schema: StructType): Unit = {
    if (!schema.fields.map(_.name).contains(columnName)) {
      throw new SparkCubeException(cacheId, s"Invalid period build request," +
        s" column[$columnName] does not exists in view" +
        s" fields[${schema.fields.map(_.name).mkString(",")}]")
    }

    schema.fields.map(field => (field.name -> field.dataType)).toMap.get(columnName) match {
      case Some(LongType) =>
      case _ =>
        throw new SparkCubeException(cacheId, s"Invalid period build request, incremental" +
          s" build by timestamp only support long type column which represents timestamp" +
          s" while column[$columnName] is not Long type.")
    }
  }
}

case class BuildHistory(
    startTime: Long,
    endTime: Long,
    buildInfo: Option[BuildInfo],
    saveMode: SaveMode,
    status: String,
    errorMsg: String) {
  override def toString: String = {
    val start = CacheUtils.formatTime(startTime)
    val end = CacheUtils.formatTime(endTime)
    s"StartTime: $start, EndTime: $end, BuildInfo: ${buildInfo.map(_.toString)}," +
      s" Save Mode: ${saveMode.name}"
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl.mmlib.algs

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions => F}
import org.joda.time.DateTime

import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.ets.alg.BaseAlg
import tech.mlsql.ets.algs.SQLAutoML

/**
 * Created by allwefantasy on 25/7/2018.
 */
trait MllibFunctions extends BaseAlg with Logging with WowLog with Serializable {

  def formatOutput(newDF: DataFrame) = {
    val schema = newDF.schema

    def formatMetrics(field: StructField, row: Row) = {
      val value = row.getSeq[Row](schema.fieldIndex(field.name))
      value.map(row => s"${row.getString(0)}:  ${row.getDouble(1)}").mkString("\n")
    }

    def formatDate(field: StructField, row: Row) = {
      val value = row.getLong(schema.fieldIndex(field.name))
      new DateTime(value).toString("yyyyMMdd mm:HH:ss:SSS")
    }

    val rows = newDF.collect().flatMap { row =>
      List(Row.fromSeq(Seq("---------------", "------------------"))) ++ schema.fields.map { field =>
        val value = field.name match {
          case "metrics" => formatMetrics(field, row)
          case "startTime" | "endTime" => formatDate(field, row)
          case _ => row.get(schema.fieldIndex(field.name)).toString
        }
        Row.fromSeq(Seq(field.name, value))
      }

    }
    val newSchema = StructType(Seq(StructField("name", StringType), StructField("value", StringType)))
    newDF.sparkSession.createDataFrame(newDF.sparkSession.sparkContext.parallelize(rows, 1), newSchema)
  }

  def formatOutputWithMultiColumns(pathPrefix: String, newDF: DataFrame) = {

    val formatMetrics = F.udf((value: Seq[Row]) => {
      value.map(row => s"${row.getString(0)}:  ${row.getDouble(1)}").mkString("\n")
    }, StringType)

    val formatDate = F.udf((value: Long) => {
      new DateTime(value).toString("yyyyMMdd mm:HH:ss:SSS")
    }, StringType)

    val formatAlg = F.udf((value: String) => {
      value.split("\\.").last
    }, StringType)

    val formatPath = F.udf((value: String) => {
      PathFun(pathPrefix).add(value).toPath
    }, StringType)

    val formatInfo = F.udf((value: String) => {
      value
    }, StringType)


    val resDF = newDF.withColumn("metrics", formatMetrics(F.col("metrics"))).
      withColumn("startTime", formatDate(F.col("startTime"))).
      withColumn("endTime", formatDate(F.col("endTime"))).
      withColumn("alg", formatAlg(F.col("alg"))).
      withColumn("modelPath", formatPath(F.col("modelPath"))).
      withColumn("message", formatInfo(F.col("message")))

    resDF

  }

  def findBestModelPath(modelList: Array[Row], algoIndex: Option[Int], baseModelPath: String, autoSelectByMetric: String) = {
    var algIndex = algoIndex
    val bestModelPath = algIndex match {
      case Some(i) => Seq(baseModelPath + "/" + i)
      case None =>
        modelList.map { row =>
          var metric: Row = null
          val metrics = row(3).asInstanceOf[scala.collection.mutable.WrappedArray[Row]]
          if (metrics.size > 0) {
            val targeMetrics = metrics.filter(f => f.getString(0) == autoSelectByMetric)
            if (targeMetrics.size > 0) {
              metric = targeMetrics.head
            } else {
              metric = metrics.head
              logInfo(format(s"No target metric: ${autoSelectByMetric} is found, use the first metric: ${metric.getDouble(1)}"))
            }
          }
          val metricScore = if (metric == null) {
            logInfo(format("No metric is found, system  will use first model"))
            0.0
          } else {
            metric.getAs[Double](1)
          }
          // if the model path contain __auto_ml__ that means, it is trained by autoML
          var baseModelPathTmp = PathFun.joinPath(baseModelPath, row(0).asInstanceOf[String].split(PathFun.pathSeparator).last)
          if (row(0).asInstanceOf[String].split(PathFun.pathSeparator)(1).contains(SQLAutoML.pathPrefix)) {
            baseModelPathTmp = baseModelPath + row(0).asInstanceOf[String]
          }
          (metricScore, row(0).asInstanceOf[String], row(1).asInstanceOf[Int], baseModelPathTmp)
        }
          .toSeq
          .sortBy(f => f._1)(Ordering[Double].reverse)
          .take(1)
          .map(f => {
            algIndex = Option(f._3)
            val baseModelPathTmp = f._4
            baseModelPathTmp
          })
    }
    bestModelPath
  }

  def autoMLfindBestModelPath(basePath: String, params: Map[String, String], sparkSession: SparkSession): Seq[String] = {
    val d = new File(basePath)
    if (!d.exists || !d.isDirectory) {
      return Seq.empty
    }
    val allETName = MLMapping.getAllETNames
    val algo_paths = d.listFiles().filter(f => {
      val path = f.getPath.split("__").last
      f.isDirectory && f.getPath.contains(SQLAutoML.pathPrefix) && allETName.contains(path)
    }).map(_.getPath).toList
    val autoSelectByMetric = params.getOrElse("autoSelectByMetric", "f1")
    var allModels = Array[Row]()
    allModels = algo_paths.map(path => {
      val (baseModelPath, metaPath) = getBaseModelPathAndMetaPath(path, params)
      val algo_name = path.split(PathFun.pathSeparator).last
      (baseModelPath, metaPath, algo_name)
    }).map(paths => {
      val baseModelPath = paths._1
      val metaModelPath = paths._2
      val modelList = sparkSession.read.parquet(PathFun(metaModelPath).add("0").toPath).collect()
      modelList.map(t => {
        val s = t.toSeq
        Row.fromSeq((s.take(0) :+ PathFun.pathSeparator + paths._3 + s(0).asInstanceOf[String]) ++ s.drop(1))
      })
    }).reduce((x, y) => {
      x ++ y
    })
    val algIndex = params.get("algIndex").map(f => f.toInt)
    val bestModelPath = findBestModelPath(allModels, algIndex, basePath, autoSelectByMetric)
    bestModelPath
  }

  def getBaseModelPathAndMetaPath(path: String, params: Map[String, String]): (String, String) = {
    val maxVersion = SQLPythonFunc.getModelVersion(path)
    val versionEnabled = maxVersion match {
      case Some(v) => true
      case None => false
    }
    val modelVersion = params.getOrElse("modelVersion", maxVersion.getOrElse(-1).toString).toInt

    val baseModelPath = if (modelVersion == -1) SQLPythonFunc.getAlgModelPath(path, versionEnabled)
    else SQLPythonFunc.getAlgModelPathWithVersion(path, modelVersion)


    val metaPath = if (modelVersion == -1) SQLPythonFunc.getAlgMetalPath(path, versionEnabled)
    else SQLPythonFunc.getAlgMetalPathWithVersion(path, modelVersion)
    (baseModelPath, metaPath)
  }

  def mllibModelAndMetaPath(path: String, params: Map[String, String], sparkSession: SparkSession) = {
    if (!isModelPath(path)) throw new MLSQLException(s"$path is not a validate model path")
    var algIndex = params.get("algIndex").map(f => f.toInt)
    val (baseModelPath, metaPath) = getBaseModelPathAndMetaPath(path, params)
    val autoSelectByMetric = params.getOrElse("autoSelectByMetric", "f1")
    val modelList = sparkSession.read.parquet(metaPath + "/0").collect()
    val bestModelPath = findBestModelPath(modelList, algIndex, baseModelPath, autoSelectByMetric)
    (bestModelPath, baseModelPath, metaPath)
  }

  def refMetaPathFromModelPath(modelPath: String) = {
    val splitedPaths = modelPath.split(PathFun.pathSeparator)
    splitedPaths.last
  }

  def saveMllibTrainAndSystemParams(sparkSession: SparkSession, params: Map[String, String], metaPath: String) = {
    val tempRDD = sparkSession.sparkContext.parallelize(Seq(Seq(Map[String, String](), params)), 1).map { f =>
      Row.fromSeq(f)
    }
    sparkSession.createDataFrame(tempRDD, StructType(Seq(
      StructField("systemParam", MapType(StringType, StringType)),
      StructField("trainParams", MapType(StringType, StringType))))).
      write.
      mode(SaveMode.Overwrite).
      parquet(metaPath + "/1")
  }

}

case class MetricValue(name: String, value: Double)
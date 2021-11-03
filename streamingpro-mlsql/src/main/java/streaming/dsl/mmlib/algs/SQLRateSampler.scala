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

import org.apache.spark.Partitioner
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions => F}
import streaming.dsl.auth.TableAuthResult
import streaming.dsl.mmlib.{Code, Doc, MarkDownDoc, ModelType, ProcessType, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.param.WowParams
import streaming.log.WowLog
import tech.mlsql.common.form.{Extra, FormParams, Text}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod


private class SQLRateSampler(override val uid: String) extends SQLAlg with Functions with WowParams with Logging
  with WowLog with ETAuth {

  def this() = this(Identifiable.randomUID("streaming.dsl.mmlib.algs.SQLRateSampler"))

  private def internal_train(df: DataFrame, params: Map[String, String]) = {

    val getIntFromRow = (row: Row, i: Int) => {
      row.get(i) match {
        case a: Int => a.asInstanceOf[Int]
        case a: Double => a.asInstanceOf[Double].toInt
        case a: Float => a.asInstanceOf[Float].toInt
        case a: Long => a.asInstanceOf[Long].toInt
        case _ => throw new MLSQLException("The type of labelCol should be int/double/float/long")
      }
    }

    val getIntFromRowByName = (row: Row, name: String) => {
      getIntFromRow(row, row.fieldIndex(name))
    }

    val labelCol = params.getOrElse("labelCol", "label")
    val isSplitWithSubLabel = params.getOrElse("isSplitWithSubLabel", "")

    // 0.8 0.1 0.1
    val sampleRates = params.getOrElse("sampleRate", "0.9,0.1").split(",").map(f => f.toDouble)
    // sampleRates validation
    if ( Math.abs(sampleRates.sum - 1.000) > 0.001 || sampleRates.length != 2)
      throw new RuntimeException("Sum of sampleRates should equal to 1 and number of elements should be 2")


    var basicRate = sampleRates.head
    val newSampleRates = sampleRates.zipWithIndex.map { sr =>
      if (sr._2 > 0) {
        basicRate = sr._1 + basicRate
        (basicRate, sr._2)
      }
      else sr
    }
    val labelToCountSeq = df.groupBy(labelCol).agg(F.count(labelCol).as("subLabelCount")).orderBy(F.asc("subLabelCount")).
      select(labelCol, "subLabelCount").collect().map { f =>
      (getIntFromRow(f, 0), f.getLong(1))
    }
    val forLog = labelToCountSeq.map(f => s"${f._1}:${f._2}").mkString(",")
    logInfo(format(s"computing data stat:${forLog}"))

    val labelCount = labelToCountSeq.size

    val labelPartionMap = labelToCountSeq.map(_._1).zipWithIndex.toMap

    if (isSplitWithSubLabel == "true") {

      val dfArray = df.rdd.map { f =>
        (getIntFromRowByName(f, labelCol), f)
      }.collect()

      val splitWithSubLabel = dfArray.groupBy(_._1).flatMap(data => {

        val groupCount = data._2.length
        var splitIndex = 0
        var beginIndex = 0
        sampleRates.flatMap(percent => {
          val takeCount = (percent * groupCount).toInt
          val endIndex = beginIndex + takeCount
          val splitData = data._2.slice(beginIndex, endIndex).map(wow => {
            Row.fromSeq(wow._2.toSeq ++ Seq(splitIndex))
          })
          splitIndex = splitIndex + 1
          beginIndex = endIndex + 1
          splitData
        })
      }).toSeq


      df.sparkSession.createDataFrame(df.sparkSession.sparkContext.parallelize(splitWithSubLabel), StructType(
        df.schema ++ Seq(StructField("__split__", IntegerType))))

    } else {
      val dfWithLabelPartition = df.rdd.map { f =>
        (getIntFromRowByName(f, labelCol), f)
      }.partitionBy(new Partitioner {
        override def numPartitions: Int = labelCount

        override def getPartition(key: Any): Int = {
          labelPartionMap.getOrElse(key.asInstanceOf[Int], 0)
        }
      }).mapPartitions { iter =>
        val r = scala.util.Random
        iter.map { wow =>
          val item = r.nextFloat()
          val index = newSampleRates.filter { sr =>
            if (sr._2 > 0) {
              val newRate = sr._1
              item <= newRate && item > newSampleRates(sr._2 - 1)._1
            }
            else {
              item <= sr._1
            }
          }.head._2
          Row.fromSeq(wow._2.toSeq ++ Seq(index))
        }
      }
      df.sparkSession.createDataFrame(dfWithLabelPartition, StructType(
        df.schema ++ Seq(StructField("__split__", IntegerType))
      ))

    }

  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val newDF = internal_train(df, params)
    newDF
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    List.empty[TableAuthResult]
  }

  final val labelCol: Param[String] = new Param[String](parent = this
    , name = "labelCol"
    , doc = FormParams.toJson( Text(
      name = "labelCol"
      , value = ""
      , extra = Extra(
        doc = "The grouping column in the dataset"
        , label = "labelCol"
        , options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )
      )
    )
    )
  )
  setDefault(labelCol, "label")

  final val sampleRate: Param[String] = new Param[String](parent = this, name = "sampleRate",
    doc = FormParams.toJson( Text ( name = "sampleRate", value = "", extra = Extra(
      doc = "comma delimited double array, i.e 0.9,0.1"
      , label = "sampleRate"
      , options = Map(
        "valueType" -> "array[double]",
        "required" -> "true",
        "derivedType" -> "NONE"
      ))
    ))
  )
  setDefault(sampleRate, "0.9,0.1")

  final val isSplitWithSubLabel: Param[String] = new Param[String](parent = this, name = "isSplitWithSubLabel",
    doc = FormParams.toJson( Text(name = "isSplitWithSubLabel", value = "", extra = Extra(
      doc = "Exact splitting if true"
      , label = "isSplitWithSubLabel"
      , options= Map(
        "valueType" -> "string",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )))
  )

  override def modelType: ModelType = ProcessType

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def doc: Doc = Doc(MarkDownDoc,
    """
      | Splits dataset into train and test, splitting ratio is specified
      | by parameter sampleRate.
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |Load a JSON data to the `data` table:
      |
      |```sql
      |set jsonStr='''
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0},
      |{"features":[5.1,3.5,1.4,0.2],"label":1.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |{"features":[4.4,2.9,1.4,0.2],"label":0.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":1.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |{"features":[4.7,3.2,1.3,0.2],"label":1.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0}
      |''';
      |load jsonStr.`jsonStr` as data;
      |
      |
      |train data as RateSampler.``
      |where labelCol="label"
      |and sampleRate="0.7,0.3" as marked_dataset;;
      |```
    """.stripMargin)
}
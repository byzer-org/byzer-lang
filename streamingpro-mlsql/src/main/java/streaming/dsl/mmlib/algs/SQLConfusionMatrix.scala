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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.MetaConst._

import Array._


/**
  * Created by dxy_why on 2018/6/16.
  */
class SQLConfusionMatrix extends SQLAlg with Functions {

  def detail(lable: String, trainLableMap: Map[Int, String], confusionMatrixArray: Array[Array[Int]]):
  Array[ConfusionMatrix] = {
    val lableIdxMap = trainLableMap.map(f => {
      (f._2, f._1)
    })
    val lable_idx = lableIdxMap(lable)
    val tp = confusionMatrixArray(lable_idx)(lable_idx)
    val fn = confusionMatrixArray(lable_idx).sum - tp
    var fp = 0
    var total = 0
    for (i <- 0 until confusionMatrixArray(lable_idx).length) {
      fp += confusionMatrixArray(i)(lable_idx)
      total += confusionMatrixArray(i).sum
    }
    fp -= tp
    val tn = total - tp - fn - fp
    val tpr = tp.toDouble / (tp + fn)
    val spc = tn.toDouble / (fp + tn)
    val ppv = tp.toDouble / (tp + fp)
    val npv = tn.toDouble / (tn + fn)
    val fpr = fp.toDouble / (fp + tn)
    val fdr = 1 - ppv
    val fnr = fn.toDouble / (fn + tp)
    val acc = (tp.toDouble + tn.toDouble) / total
    Array[ConfusionMatrix](
      ConfusionMatrix(lable, "TP", tp.toString, "True positive [eqv with hit]"),
      ConfusionMatrix(lable, "TN", tn.toString, "True negative [eqv with correct rejection]"),
      ConfusionMatrix(lable, "FP", fp.toString, "False positive [eqv with false alarm, Type I error]"),
      ConfusionMatrix(lable, "FN", fn.toString, "False negative [eqv with miss, Type II error]"),
      ConfusionMatrix(lable, "TPR", tpr.toString, "Sensitivity or true positive rate [eqv with hit rate, recall]"),
      ConfusionMatrix(lable, "SPC", spc.toString, "Specificity or true negative rate"),
      ConfusionMatrix(lable, "PPV", ppv.toString, "Precision or positive prediction value"),
      ConfusionMatrix(lable, "NPV", npv.toString, "Negative predictive value"),
      ConfusionMatrix(lable, "FPR", fpr.toString, "Fall-out or false positive rate"),
      ConfusionMatrix(lable, "FDR", fdr.toString, "False discovery rate"),
      ConfusionMatrix(lable, "FNR", fnr.toString, "Miss Rate or False Negative Rate"),
      ConfusionMatrix(lable, "ACC", acc.toString, "Accuracy"))
  }

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val spark = df.sparkSession
    import spark.implicits._
    val path = params("path")
    val actualCol = params.getOrElse(ConfusionMatrixParamsConstrant.ACTUAL_COLUMN, "")
    val predictCol = params.getOrElse(ConfusionMatrixParamsConstrant.PREDICT_COLUMN, "")
    require(df.schema.fieldNames.contains(actualCol) && df.schema.fieldNames.contains(predictCol),
      "真实标签列 或者 预测标签列 为空")
    val trainDataMap = df.select(actualCol, predictCol)
      .toDF("actualCol", "predictCol")
      .as[ConfusionMatrixTrainData].rdd.map(f => {
      ((f.actualCol, f.predictCol), 1)
    }).reduceByKey((s1, s2) => {
      s1 + s2
    }).collect().toMap

    val trainLable = (trainDataMap.map(_._1._1).toSet union trainDataMap.map(_._1._2).toSet).toArray.sorted
    val trainLableCount = trainLable.size
    val trainLableMap = trainLable.zipWithIndex.map(f => {
      (f._2, f._1)
    }).toMap
    val confusionMatrixArray = ofDim[Int](trainLableCount, trainLableCount)
    for (actualIdx <- 0 until trainLableCount) {
      for (predictIdx <- 0 until trainLableCount) {
        val actualLabel = trainLableMap(actualIdx)
        val predictLabel = trainLableMap(predictIdx)
        confusionMatrixArray(actualIdx)(predictIdx) = trainDataMap.getOrElse((actualLabel, predictLabel), 0)
      }
    }
    val confusionMatrixSeq = trainLable.zip(confusionMatrixArray).map(f => {
      Row.fromSeq((f._1 +: f._2.map(_.toString)).toSeq)
    }).toSeq
    val schema = StructType(
      ("act\\prt" +: trainLable).map(lable => {
        StructField(lable, StringType, true)
      }).toList)
    val rdd = spark.sparkContext.parallelize(confusionMatrixSeq)
    val resultDataframe = spark.createDataFrame(rdd, schema)
    resultDataframe.write.mode(SaveMode.Overwrite).parquet(getDataPath(path))
    val confusionMatrix = trainLable.flatMap(lable => {
      detail(lable, trainLableMap, confusionMatrixArray)
    })
    spark.createDataset(confusionMatrix).write.mode(SaveMode.Overwrite).
      parquet(s"${path.stripSuffix("/")}/detail")


  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    internal_train(df, params + ("path" -> path))
    emptyDataFrame()(df)
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {

  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    MLSQLUtils.createUserDefinedFunction(null, ArrayType(DoubleType), Some(Seq(ArrayType(DoubleType))))
  }
}

case class ConfusionMatrix(lable: String, name: String, value: String, desc: String)

case class ConfusionMatrixTrainData(actualCol: String, predictCol: String)

object ConfusionMatrixParamsConstrant {
  /**
    * 真实标签列
    */
  val ACTUAL_COLUMN = "actualCol"
  /**
    * 预测标签列
    */
  val PREDICT_COLUMN = "predictCol"
}

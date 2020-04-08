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

import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg
import MetaConst._
import org.apache.spark.ml.help.HSQLStringIndex
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.algs.meta.Word2IndexMeta
import org.apache.spark.ml.linalg.SQLDataTypes._

/**
  * Created by allwefantasy on 18/6/2018.
  */
class SQLVecMapInPlace extends SQLAlg with Functions {

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val path = params("path")
    val metaPath = getMetaPath(path)
    saveTraningParams(df.sparkSession, params, metaPath)

    val spark = df.sparkSession
    import spark.implicits._

    val inputCol = params.getOrElse("inputCol", "")
    require(!inputCol.isEmpty, "inputCol is required when use SQLVecMapInPlace")

    val keyRDD = df.rdd.flatMap { f =>
      f.getMap[String, Number](f.fieldIndex(inputCol)).keys.toArray[String]
    }.map(f => Row(Seq(f)))

    val keyDF = spark.createDataFrame(keyRDD, StructType(Seq(
      StructField(inputCol, ArrayType(StringType))
    )))
    //train word index
    val si = new SQLStringIndex()
    si.train(keyDF, MetaConst.WORD_INDEX_PATH(metaPath, inputCol), params)
    val model = si.load(spark, MetaConst.WORD_INDEX_PATH(metaPath, inputCol), params)
    val word2IndexMapping = HSQLStringIndex.wordToIndex(spark, model)
    val featureSize = word2IndexMapping.size

    val f = (item: Map[String, Number]) => {
      val elements = item.map(f => (word2IndexMapping(f._1).toInt, f._2.doubleValue())).toSeq
      Vectors.sparse(featureSize, elements)
    }
    val fUDF = F.udf(f)
    val newDF = df.withColumn(inputCol, fUDF(F.col(inputCol)))
    newDF
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val newDF = internal_train(df, params + ("path" -> path))
    newDF.write.mode(SaveMode.Overwrite).parquet(getDataPath(path))
    emptyDataFrame()(df)
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    import spark.implicits._
    //load train params
    val path = getMetaPath(_path)
    val df = spark.read.parquet(PARAMS_PATH(path, "params")).map(f => (f.getString(0), f.getString(1)))
    val trainParams = df.collect().toMap
    val inputCol = trainParams.getOrElse("inputCol", "")

    val si = new SQLStringIndex()
    val model = si.load(spark, MetaConst.WORD_INDEX_PATH(path, inputCol), params)
    val word2IndexMapping = HSQLStringIndex.wordToIndex(spark, model)
    Word2IndexMeta(trainParams, word2IndexMapping.toMap)

  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val word2IndexMeta = _model.asInstanceOf[Word2IndexMeta]
    val word2IndexMappingBr = sparkSession.sparkContext.broadcast(word2IndexMeta.wordIndex)
    val featureSize = word2IndexMappingBr.value.size
    val f = (item: Map[String, Double]) => {
      val elements = item.map(f => (word2IndexMappingBr.value(f._1).toInt, f._2)).toSeq
      Vectors.sparse(featureSize, elements)
    }
    MLSQLUtils.createUserDefinedFunction(f, VectorType, Some(Seq(MapType(StringType, DoubleType))))
  }
}

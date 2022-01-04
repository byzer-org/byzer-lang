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

import org.apache.spark
import org.apache.spark.ml.feature.{Bucketizer, DiscretizerFeature, QuantileDiscretizer}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SaveMode, SparkSession}
import streaming.dsl.mmlib.{Code, Doc, HtmlDoc, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.meta.DiscretizerMeta
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.form.{Extra, FormParams, KV, Select}
import tech.mlsql.dsl.TagParamName
import tech.mlsql.dsl.auth.BaseETAuth
import tech.mlsql.version.VersionCompatibility

class SQLDiscretizer(override val uid: String) extends SQLAlg with Functions with VersionCompatibility with WowParams with BaseETAuth {
  def this() = this(BaseParams.randomUID())

  def trainByGroup(df: DataFrame, params: Map[String, String], _method: String): Unit = {
    val fitParamsWithIndex = arrayParamsWithIndex(DiscretizerParamsConstrant.PARAMS_PREFIX, params)
    require(fitParamsWithIndex.size > 0, "fitParams should be configured")
    val dfWithId = df.withColumn("id", monotonically_increasing_id)
    var transformedDF = dfWithId
    // we need save metadatas with index, because we need index
    val metas: Array[(Int, DiscretizerTrainData)] = {
      _method match {
        case DiscretizerFeature.BUCKETIZER_METHOD =>
          fitParamsWithIndex.map {
            case (index, map) =>
              val bucketizer = new Bucketizer()
              val splitArray = DiscretizerFeature.getSplits(
                map.getOrElse(DiscretizerParamsConstrant.SPLIT_ARRAY, "")
              )
              configureModel(bucketizer, map)
              val outputCols = bucketizer match {
                case i if i.isSet(i.outputCol) => Array("id", bucketizer.getOutputCol) // if the outputCol is setted then get value from the setted outputCol
                case i if i.isSet(i.outputCols) => Array("id") ++ bucketizer.getOutputCols // if the outputCols are settedm
                case i if i.isDefined(i.outputCol) => Array("id", bucketizer.getOutputCol)
                case i if i.isDefined(i.outputCols) => Array("id") ++ bucketizer.getOutputCols
                case _ => Array("")
              }
              val tmpDF = bucketizer.transform(dfWithId).select(outputCols.toList.head, outputCols.tail: _*)
              transformedDF = transformedDF.join(tmpDF, tmpDF("id") === transformedDF("id")).drop(tmpDF("id"))
              val splits = bucketizer.getSplits
              (index, DiscretizerFeature.parseParams(map, splits))
          }

        case DiscretizerFeature.QUANTILE_METHOD =>
          fitParamsWithIndex.map {
            case (index, map) =>
              val discretizer = new QuantileDiscretizer()
              configureModel(discretizer, map)
              val outputCols = discretizer match {
                case i if i.isDefined(i.outputCol) => Array("id", discretizer.getOutputCol)
                case i if i.isDefined(i.outputCols) => Array("id") ++ discretizer.getOutputCols
                case _ => Array("")
              }
              val discretizerModel = discretizer.fit(df)
              val tmpDF = discretizerModel.transform(dfWithId).select(outputCols.toList.head, outputCols.tail: _*)
              transformedDF = transformedDF.join(tmpDF, tmpDF("id") === transformedDF("id")).drop(tmpDF("id"))
              val splits = discretizerModel.getSplits
              (index, DiscretizerFeature.parseParams(map, splits))
          }
      }
    }
    return metas
  }

  def checkWithoutGroupParams(params: Map[String, String]): Unit = {
    val group = params.map(p => {
      val key = p._1
      key.contains(DiscretizerParamsConstrant.PARAMS_PREFIX + ".")
    }).filter(_ == true)
    require(group.size == 0, "The multi-group params are not available for the Discretizer Et!")
  }

  def trainWithoutGroup(df: DataFrame, params: Map[String, String], _method: String, metaPath: String): DataFrame = {
    val dfWithId = df.withColumn("id", monotonically_increasing_id)
    checkWithoutGroupParams(params)
    var transformedDF = dfWithId
    // we need save metadatas with index, because we need index
    val metas: Array[(Int, DiscretizerTrainData)] = {
      _method match {
        case DiscretizerFeature.BUCKETIZER_METHOD => {
          val bucketizer = new Bucketizer()
          configureModel(bucketizer, params)
          val outputCols = bucketizer match {
            case i if i.isSet(i.outputCol) => Array("id", bucketizer.getOutputCol) // if the outputCol is setted then get value from the setted outputCol
            case i if i.isSet(i.outputCols) => Array("id") ++ bucketizer.getOutputCols // if the outputCols are settedm
            case i if i.isDefined(i.outputCol) => Array("id", bucketizer.getOutputCol)
            case i if i.isDefined(i.outputCols) => Array("id") ++ bucketizer.getOutputCols
            case _ => Array("")
          }
          val tmpDF = bucketizer.transform(dfWithId).select(outputCols.toList.head, outputCols.tail: _*)
          transformedDF = transformedDF.join(tmpDF, tmpDF("id") === transformedDF("id")).drop(tmpDF("id"))
          val splits = bucketizer.getSplits
          Array((0, DiscretizerFeature.parseParams(params, splits)))
        }

        case DiscretizerFeature.QUANTILE_METHOD => {
          val discretizer = new QuantileDiscretizer()
          configureModel(discretizer, params)
          val outputCols = discretizer match {
            case i if i.isDefined(i.outputCol) => Array("id", discretizer.getOutputCol)
            case i if i.isDefined(i.outputCols) => Array("id") ++ discretizer.getOutputCols
            case _ => Array("")
          }
          val discretizerModel = discretizer.fit(df)
          val tmpDF = discretizerModel.transform(dfWithId).select(outputCols.toList.head, outputCols.tail: _*)
          transformedDF = transformedDF.join(tmpDF, tmpDF("id") === transformedDF("id")).drop(tmpDF("id"))
          val splits = discretizerModel.getSplits
          Array((0, DiscretizerFeature.parseParams(params, splits)))
        }
      }
    }
    val spark = df.sparkSession
    import spark.implicits._
    spark.createDataset(metas).write.mode(SaveMode.Overwrite).parquet(DISCRETIZER_PATH(metaPath))
    transformedDF.drop("id")
  }

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val spark = df.sparkSession
    import spark.implicits._
    val path = params("path")
    val metaPath = getMetaPath(path)

    val _method = params.getOrElse(method.name, DiscretizerFeature.BUCKETIZER_METHOD)
    set(method, _method)

    saveTraningParams(df.sparkSession, params, metaPath)
    trainWithoutGroup(df, params, _method, metaPath)
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    internal_train(df, params + ("path" -> path))
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    import spark.implicits._
    val path = getMetaPath(_path)

    val metas = spark.read
      .parquet(DISCRETIZER_PATH(path))
      .as[(Int, DiscretizerTrainData)]
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val func = DiscretizerFeature.getDiscretizerPredictFun(spark, metas)
    DiscretizerMeta(metas, func)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val meta = _model.asInstanceOf[DiscretizerMeta]
    MLSQLUtils.createUserDefinedFunction(meta.discretizerFunc, ArrayType(DoubleType), Some(Seq(ArrayType(DoubleType))))
  }

  override def supportedVersions: Seq[String] = Seq(">=1.6.0")

  override def etName() = "discretizer"


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainTagParams(sparkSession, () => {
      Map(
        TagParamName(method.name, DiscretizerFeature.BUCKETIZER_METHOD) -> new Bucketizer(),
        TagParamName(method.name, DiscretizerFeature.QUANTILE_METHOD) -> new QuantileDiscretizer()
      )
    })
  }

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="https://en.wikipedia.org/wiki/Discretization">Discretization</a>
      |In applied mathematics, discretization is the process of transferring continuous functions,
      |models, variables, and equations into discrete counterparts.
      |This process is usually carried out as a first step toward making them suitable for numerical
      |evaluation and implementation on digital computers. Dichotomization is the special case of
      |discretization in which the number of discrete classes is 2,
      |which can approximate a continuous variable as a binary variable
      |(creating a dichotomy for modeling purposes, as in binary classification).
      |
      | Use "load modelParams.`Discretizer` as output;"
      |
      | to check the available hyper parameters;
      |
      |
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
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
      |select features[0] as a ,features[1] as b from data
      |as data1;
      |
      |train data1 as Discretizer.`/tmp/model`
      |where method="bucketizer"
      |and `inputCol`="a"
      |and `splits`="-inf,0.0,1.0,inf";
      |
      |register Discretizer.`/tmp/model` as convert;
      |select convert(array(double(7))) as features as output;
      |
      |
      |train data1 as Discretizer.`/tmp/model`
      |where method="bucketizer"
      |and `inputCol`="a"
      |and `splits`="-inf,0.0,1.0,inf";
      |
      |register Discretizer.`/tmp/model` as convert1;
      |select convert1(array(double(7))) as features as output;
      |
      |;
    """.stripMargin)

  val method: Param[String] = new Param[String](this, "method", FormParams.toJson(
    Select(
      name = "method",
      values = List(),
      extra = Extra(
        doc = "",
        label = "",
        options = Map(
        )), valueProvider = Option(() => {
        List(
          KV(Some("method"), Some(DiscretizerFeature.BUCKETIZER_METHOD)),
          KV(Some("method"), Some(DiscretizerFeature.QUANTILE_METHOD))
        )
      })
    )
  ))
}

case class DiscretizerTrainData(
                                 inputCol: String,
                                 splits: Array[Double],
                                 handleInvalid: Boolean,
                                 params: Map[String, String])

object DiscretizerParamsConstrant {
  /**
   * The prefix of the group of params
   */
  val PARAMS_PREFIX = "fitParam"
  /**
   * The name of the input column
   */
  val INPUT_COLUMN = "inputCol"
  /**
   * the array that defines the splits param
   */
  val SPLIT_ARRAY = "splitsArray"
  /**
   * The way to discretize the input, bucketizer,quantile are allowed
   */
  val METHOD = "method"
  /**
   *
   */
  val HANDLE_INVALID = "handleInvalid"
}

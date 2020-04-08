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

import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.param.{DoubleParam, Param}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SparkSession}
import streaming.core.shared.SharedObjManager
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.feature.StringFeature
import streaming.dsl.mmlib.algs.meta.TFIDFMeta
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.dsl.mmlib.{CoreVersion, SQLAlg}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 7/5/2018.
  */
class SQLTfIdfInPlace(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val newDF = interval_train(df, params + ("path" -> path))
    newDF
  }

  def interval_train(df: DataFrame, params: Map[String, String]) = {
    params.get(dicPaths.name).
      map(m => set(dicPaths, m)).getOrElse {
      set(dicPaths, "")
    }

    params.get(inputCol.name).
      map(m => set(inputCol, m)).getOrElse {
      throw new IllegalArgumentException("inputCol is required by TfIdfInPlace")
    }

    params.get(stopWordPath.name).
      map(m => set(stopWordPath, m)).getOrElse {
      set(stopWordPath, "")
    }

    params.get(priorityDicPath.name).
      map(m => set(priorityDicPath, m)).getOrElse {
      set(priorityDicPath, "")
    }

    params.get(priority.name).
      map(m => set(priority, m.toDouble)).getOrElse {
      set(priority, 1d)
    }

    params.get(nGrams.name).
      map(m => set(nGrams, m)).getOrElse {
      set(nGrams, "")
    }

    require($(inputCol) != null, "inputCol is required when use SQLTfIdfInPlace")

    val path = params("path")
    val split = params.getOrElse("split", null)

    val metaPath = getMetaPath(path)

    // keep params
    saveTraningParams(df.sparkSession, Map("ignoreNature" -> "true") ++ params, metaPath)
    val newDF = StringFeature.tfidf(df, metaPath,
      $(dicPaths), $(inputCol), $(stopWordPath), $(priorityDicPath), $(priority),
      $(nGrams).split(",").filterNot(f => f.isEmpty).map(f => f.toInt).toSeq, split)
    newDF
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    import spark.implicits._
    //load train params
    val path = getMetaPath(_path)
    val df = spark.read.parquet(PARAMS_PATH(path, "params")).map(f => (f.getString(0), f.getString(1)))
    val trainParams = df.collect().toMap
    val inputCol = trainParams.getOrElse("inputCol", "")
    //load wordindex
    val wordIndex = spark.read.parquet(WORD_INDEX_PATH(path, inputCol)).map(f => ((f.getString(0), f.getDouble(1)))).collect().toMap
    //load tfidf model
    val tfidf = new SQLTfIdf()
    val tfidfModel = tfidf.load(df.sparkSession, TF_IDF_PATH(path, inputCol), Map())
    val tfidfFunc = tfidf.internal_predict(df.sparkSession, tfidfModel, "wow")("wow")
    TFIDFMeta(trainParams, wordIndex, tfidfFunc)
  }

  override def predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val func = internal_predict(spark, _model, name, params)
    MLSQLUtils.createUserDefinedFunction(func, VectorType, Some(Seq(StringType)))
  }

  def internal_predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]) = {
    val tfIDMeta = _model.asInstanceOf[TFIDFMeta]
    val trainParams = tfIDMeta.trainParams
    val wordIndexBr = spark.sparkContext.broadcast(tfIDMeta.wordIndex)
    val tfidfFunc = tfIDMeta.tfidfFunc

    val dicPaths = trainParams.getOrElse("dicPaths", "")
    val priorityDicPath = trainParams.getOrElse("priorityDicPath", "")
    val priority = trainParams.getOrElse("priority", "1").toDouble
    val stopWordPath = trainParams.getOrElse("stopWordPath", "")
    val nGrams = trainParams.getOrElse("nGrams", "").split(",").filterNot(f => f.isEmpty).map(f => f.toInt).toSeq
    val split = trainParams.getOrElse("split", null)

    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))

    val stopwords = StringFeature.loadStopwords(df, stopWordPath)
    val stopwordsBr = spark.sparkContext.broadcast(stopwords)
    val words = spark.sparkContext.broadcast(SQLTokenAnalysis.loadDics(spark, trainParams + ("dic.paths" -> dicPaths)))


    val (priorityWords, priorityFunc) = StringFeature.loadPriorityWords(df, priorityDicPath, priority, (str: String) => {
      wordIndexBr.value.getOrElse(str, -1d).toInt
    })

    val ngram = (words: Seq[String], n: Int) => {
      words.iterator.sliding(n).withPartial(false).map(_.mkString(" ")).toSeq
    }

    val func = (content: String) => {
      val wordArray = {
        if (split != null) {
          content.split(split)
        } else {
          // create analyser
          val forest = SharedObjManager.getOrCreate[Any](dicPaths, SharedObjManager.forestPool, () => {
            SQLTokenAnalysis.createForest(words.value, trainParams)
          })
          val parser = SQLTokenAnalysis.createAnalyzerFromForest(forest.asInstanceOf[AnyRef], trainParams)
          // analyser content
          SQLTokenAnalysis.parseStr(parser, content, trainParams).
            filter(f => !stopwordsBr.value.contains(f))
        }
      }
      //ngram
      val finalWordArray = new ArrayBuffer[String]()
      finalWordArray ++= wordArray
      nGrams.foreach { ng =>
        finalWordArray ++= ngram(wordArray, ng)
      }

      // number sequence
      val wordIntArray = finalWordArray.filter(f => wordIndexBr.value.contains(f)).map(f => wordIndexBr.value(f).toInt)

      //tfidf
      val vector = tfidfFunc(wordIntArray)

      // enhance some feature
      priorityFunc(vector)
    }
    func
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def coreCompatibility: Seq[CoreVersion] = super.coreCompatibility

  final val dicPaths: Param[String] = new Param[String](this, "dicPaths", "user-defined dictionary")
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "Which text column you want to process")
  final val stopWordPath: Param[String] = new Param[String](this, "stopWordPath", "user-defined stop word dictionary")
  final val priorityDicPath: Param[String] = new Param[String](this, "priorityDicPath", "user-defined dictionary")
  final val priority: DoubleParam = new DoubleParam(this, "priority", "how much weight should be applied in priority words")
  final val nGrams: Param[String] = new Param[String](this, "nGrams", "ngram，we can compose 2 or 3 words together so maby the new complex features can more succinctly capture importtant information in raw data. Note that too much ngram composition may increase feature space too much , this makes it hard to compute.")

}

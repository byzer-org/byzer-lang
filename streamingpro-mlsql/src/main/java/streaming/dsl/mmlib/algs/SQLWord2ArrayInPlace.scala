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
import org.apache.spark.sql.{DataFrame, MLSQLUtils, SaveMode, SparkSession}
import streaming.core.shared.SharedObjManager
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.feature.StringFeature
import streaming.dsl.mmlib.algs.feature.StringFeature.loadWordvecs
import streaming.dsl.mmlib.algs.meta.Word2ArrayMeta

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhuml on 8/8/2018.
  */
class SQLWord2ArrayInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val wordvecPaths = params.getOrElse("wordvecPaths", "")
    val wordVecMap = loadWordvecs(spark, wordvecPaths)
    import spark.implicits._
    if (wordVecMap.size > 0) {
      wordVecMap.toSeq.map(_._1).toDF("word")
    } else {
      val modelPath = params("modelPath")
      val modelMetaPath = getMetaPath(modelPath)
      val modelParams = spark.read.parquet(PARAMS_PATH(modelMetaPath, "params")).map(f => (f.getString(0), f.getString(1))).collect().toMap
      val inputCol = modelParams.getOrElse("inputCol", "")
      val wordsDf = spark.read.parquet(WORD_INDEX_PATH(modelMetaPath, inputCol)).map(f => f.getString(0)).toDF()
      saveTraningParams(df.sparkSession, params ++ modelParams, getMetaPath(path))
      wordsDf.write.mode(SaveMode.Overwrite).parquet(WORDS_PATH(getMetaPath(path)))
    }
    emptyDataFrame()(df)
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    import spark.implicits._
    //load train params
    val path = getMetaPath(_path)
    val df = spark.read.parquet(PARAMS_PATH(path, "params")).map(f => (f.getString(0), f.getString(1)))
    val trainParams = df.collect().toMap
    val wordsSet = spark.read.parquet(WORDS_PATH(path)).map(_.getString(0)).collect().toSet
    Word2ArrayMeta(trainParams, wordsSet)
  }

  override def predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val word2ArrayMeta = _model.asInstanceOf[Word2ArrayMeta]
    val trainParams = word2ArrayMeta.trainParams
    val words = spark.sparkContext.broadcast(word2ArrayMeta.words)
    val dicPaths = trainParams.getOrElse("dicPaths", "")
    val split = trainParams.getOrElse("split", null)
    val wordvecPaths = trainParams.getOrElse("wordvecPaths", "")
    val nGrams = trainParams.getOrElse("nGrams", "").split(",").filterNot(f => f.isEmpty).map(f => f.toInt).toSeq
    val wordsBr = spark.sparkContext.broadcast(SQLTokenAnalysis.loadDics(spark, trainParams) ++ StringFeature.loadDicsFromWordvec(spark, wordvecPaths))

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
            SQLTokenAnalysis.createForest(wordsBr.value, trainParams)
          })
          val parser = SQLTokenAnalysis.createAnalyzerFromForest(forest.asInstanceOf[AnyRef], trainParams)
          // analyser content
          SQLTokenAnalysis.parseStr(parser, content, trainParams)
        }
      }
      //ngram
      val finalWordArray = new ArrayBuffer[String]()
      finalWordArray ++= wordArray
      nGrams.foreach { ng =>
        finalWordArray ++= ngram(wordArray, ng)
      }

      finalWordArray.filter(f => words.value.contains(f)).toArray
    }
    MLSQLUtils.createUserDefinedFunction(func, ArrayType(StringType), Some(Seq(StringType)))
  }

}
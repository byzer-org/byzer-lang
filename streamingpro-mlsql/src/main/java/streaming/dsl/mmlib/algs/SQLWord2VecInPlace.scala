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

import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession, functions => F}
import streaming.core.shared.SharedObjManager
import streaming.dsl.mmlib.{Code, Doc, HtmlDoc, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.feature.StringFeature
import streaming.dsl.mmlib.algs.feature.StringFeature.loadWordvecs
import streaming.dsl.mmlib.algs.meta.Word2VecMeta
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, KV, Select, Text}

/**
 * Created by allwefantasy on 7/5/2018.
 */
class SQLWord2VecInPlace(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    interval_train(df, params + ("path" -> path)).write.mode(SaveMode.Overwrite).parquet(getDataPath(path))
    emptyDataFrame()(df)
  }

  final val dicPaths: Param[String] = new Param[String](this, "dicPaths", FormParams.toJson(Text(
    name = "dicPaths",
    value = "",
    extra = Extra(
      doc =
        """
          |user-defined dictionary
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )
  )))
  final val inputCol: Param[String] = new Param[String](this, "inputCol", FormParams.toJson(Text(
    name = "inputCol",
    value = "",
    extra = Extra(
      doc =
        """
          |"Which text column you want to process"
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "true",
        "derivedType" -> "NONE"
      )
    )
  )))
  final val stopWordPath: Param[String] = new Param[String](this, "stopWordPath", FormParams.toJson(Text(
    name = "stopWordPath",
    value = "",
    extra = Extra(
      doc =
        """
          |user-defined dictionary
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )
  )))
  final val wordvecPaths: Param[String] = new Param[String](this, "wordvecPaths", FormParams.toJson(Text(
    name = "wordvecPaths",
    value = "",
    extra = Extra(
      doc =
        """
          |you can specify the location of existed word2vec model
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )
  )))
  final val vectorSize: IntParam = new IntParam(this, "vectorSize", FormParams.toJson(Text(
    name = "vectorSize",
    value = "",
    extra = Extra(
      doc =
        """
          |the word vector size you expect
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "int",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )
  )))
  final val minCount: IntParam = new IntParam(this, "minCount", FormParams.toJson(Text(
    name = "minCount",
    value = "",
    extra = Extra(
      doc =
        """
          |the minimum count of the frequency of words.
          |A word whose frequency under the minCount will not generate related vector.
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "int",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )
  )))
  final val split: Param[String] = new Param[String](this, "split", FormParams.toJson(Text(
    name = "split",
    value = "",
    extra = Extra(
      doc =
        """
          |optinal, a token specifying how to analysis the text string
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )
  )))
  final val length: IntParam = new IntParam(this, "length", FormParams.toJson(Text(
    name = "length",
    value = "",
    extra = Extra(
      doc =
        """
          | input sentence length
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "int",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )
  )))
  final val resultFeature: Param[String] = new Param[String](this, "resultFeature", FormParams.toJson(Text(
    name = "resultFeature",
    value = "",
    extra = Extra(
      doc =
        """
          | flag:concat m n-dim arrays to one m*n-dim array;merge: merge multi n-dim arrays into one n-dim array；
          | index: output of conword sequence
          |""".stripMargin,
      label = "",
      options = Map(
        "valueType" -> "string",
        "required" -> "false",
        "derivedType" -> "NONE"
      )
    )
  )))

  def interval_train(df: DataFrame, params: Map[String, String]) = {

    params.get(dicPaths.name).
      map(m => set(dicPaths, m)).getOrElse {
      set(dicPaths, "")
    }

    params.get(wordvecPaths.name).
      map(m => set(wordvecPaths, m)).getOrElse {
      set(wordvecPaths, "")
    }

    params.get(inputCol.name).
      map(m => set(inputCol, m)).getOrElse {
      set(inputCol, "")
    }

    params.get(vectorSize.name).
      map(m => set(vectorSize, m.toInt)).getOrElse {
      set(vectorSize, 100)
    }

    params.get(length.name).
      map(m => set(length, m.toInt)).getOrElse {
      set(length, 100)
    }

    params.get(stopWordPath.name).
      map(m => set(stopWordPath, m)).getOrElse {
      set(stopWordPath, "")
    }

    params.get(resultFeature.name).
      map(m => set(resultFeature, m)).getOrElse {
      set(resultFeature, "")
    }


    params.get(minCount.name).
      map(m => set(minCount, m.toInt)).getOrElse {
      set(minCount, 1)
    }

    params.get(split.name).
      map(m => set(split, m)).getOrElse {
      set(split, null)
    }

    require($(inputCol) != null && $(inputCol).nonEmpty, "inputCol is required when use SQLWord2VecInPlace")
    val metaPath = getMetaPath(params("path"))
    // keep params
    saveTraningParams(df.sparkSession, params, metaPath)

    var newDF = StringFeature.word2vec(df, metaPath, $(dicPaths), $(wordvecPaths), $(inputCol), $(stopWordPath), $(resultFeature), $(split), $(vectorSize), $(length), $(minCount))
    if (resultFeature.equals("flat")) {
      val flatFeatureUdf = F.udf((a: Seq[Seq[Double]]) => {
        a.flatten
      })
      newDF = newDF.withColumn($(inputCol), flatFeatureUdf(F.col($(inputCol))))
    }

    val _vectorSize = $(vectorSize)

    if (resultFeature.equals("merge")) {
      val flatFeatureUdf = F.udf((a: Seq[Seq[Double]]) => {
        if (a.size == 0) {
          Seq[Double]()
        }
        else {
          val r = new Array[Double](_vectorSize)
          for (a1 <- a) {
            val b = a1.toList
            for (i <- 0 until b.size) {
              r(i) = b(i) + r(i)
            }
          }
          r.toSeq
        }
      })
      newDF = newDF.withColumn($(inputCol), flatFeatureUdf(F.col($(inputCol))))
    }
    newDF
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    import spark.implicits._
    //load train params
    val path = getMetaPath(_path)
    val df = spark.read.parquet(PARAMS_PATH(path, "params")).map(f => (f.getString(0), f.getString(1)))
    val trainParams = df.collect().toMap
    val inputCol = trainParams.getOrElse("inputCol", "")
    val wordvecPaths = trainParams.getOrElse("wordvecPaths", "")
    val wordvecsMap = loadWordvecs(spark, wordvecPaths)
    if (wordvecsMap.size > 0) {
      Word2VecMeta(trainParams, Map[String, Double](), null)
    } else {
      //load wordindex
      val wordIndex = spark.read.parquet(WORD_INDEX_PATH(path, inputCol)).map(f => ((f.getString(0), f.getDouble(1)))).collect().toMap
      //load word2vec model
      val word2vec = new SQLWord2Vec()
      val model = word2vec.load(spark, WORD2VEC_PATH(path, inputCol), Map())
      val predictFunc = word2vec.internal_predict(df.sparkSession, model, "wow")("wow_array").asInstanceOf[(Seq[String]) => Seq[Seq[Double]]]
      Word2VecMeta(trainParams, wordIndex, predictFunc)
    }
  }

  override def predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val word2vecMeta = _model.asInstanceOf[Word2VecMeta]
    val trainParams = word2vecMeta.trainParams
    val dicPaths = trainParams.getOrElse("dicPaths", "")
    val stopWordPath = trainParams.getOrElse("stopWordPath", "")
    val wordvecPaths = trainParams.getOrElse("wordvecPaths", "")
    val resultFeature = trainParams.getOrElse("resultFeature", "")
    val vectorSize = trainParams.getOrElse("vectorSize", "100").toInt
    val length = trainParams.getOrElse("length", "100").toInt
    val wordIndexBr = spark.sparkContext.broadcast(word2vecMeta.wordIndex)
    val split = trainParams.getOrElse("split", null)

    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
    val stopwords = StringFeature.loadStopwords(df, stopWordPath)
    val stopwordsBr = spark.sparkContext.broadcast(stopwords)
    val wordVecsBr = spark.sparkContext.broadcast(StringFeature.loadWordvecs(spark, wordvecPaths))
    val wordsArrayBr = spark.sparkContext.broadcast(StringFeature.loadDicsFromWordvec(spark, wordvecPaths))
    val wordArrayFunc = (content: String) => {
      if (split != null) {
        content.split(split)
      } else {
        // create analyser
        val forest = SharedObjManager.getOrCreate[Any](dicPaths, SharedObjManager.forestPool, () => {
          val words = SQLTokenAnalysis.loadDics(spark, trainParams) ++ wordsArrayBr.value
          SQLTokenAnalysis.createForest(words, trainParams)
        })
        val parser = SQLTokenAnalysis.createAnalyzerFromForest(forest.asInstanceOf[AnyRef], trainParams)
        // analyser content
        SQLTokenAnalysis.parseStr(parser, content, trainParams).
          filter(f => !stopwordsBr.value.contains(f))
      }
    }
    val func = (content: String) => {
      val wordArray = wordArrayFunc(content)
      if (wordVecsBr.value.size > 0) {
        val r = new Array[Seq[Double]](length)
        val wordvecsMap = wordVecsBr.value
        val wSize = wordArray.size
        for (i <- 0 until length) {
          if (i < wSize && wordvecsMap.contains(wordArray(i))) {
            r(i) = wordvecsMap(wordArray(i))
          } else
            r(i) = new Array[Double](vectorSize)
        }
        r.toSeq
      }
      else {
        val wordIntArray = wordArray.filter(f => wordIndexBr.value.contains(f)).map(f => wordIndexBr.value(f).toInt)
        word2vecMeta.predictFunc(wordIntArray.map(f => f.toString).toSeq)
      }
    }


    val funcIndex = (content: String) => {
      val wordArray = wordArrayFunc(content)
      wordArray.filter(f => wordIndexBr.value.contains(f)).map(f => wordIndexBr.value(f).toInt)
    }

    resultFeature match {
      case "flat" => {
        val f2 = (a: String) => {
          func(a).flatten
        }
        MLSQLUtils.createUserDefinedFunction(f2, ArrayType(DoubleType), Some(Seq(StringType)))
      }
      case "merge" => {
        val f2 = (a: String) => {
          val seq = func(a)
          if (seq.size == 0) {
            Seq[Double]()
          } else {
            val r = new Array[Double](vectorSize)
            for (a1 <- seq) {
              val b = a1.toList
              for (i <- 0 until b.size) {
                r(i) = b(i) + r(i)
              }
            }
            r.toSeq
          }
        }
        MLSQLUtils.createUserDefinedFunction(f2, ArrayType(DoubleType), Some(Seq(StringType)))
      }
      case _ => {
        if (wordVecsBr.value.size == 0 && resultFeature.equals("index"))
          MLSQLUtils.createUserDefinedFunction(funcIndex, ArrayType(IntegerType), Some(Seq(StringType)))
        else
          MLSQLUtils.createUserDefinedFunction(func, ArrayType(ArrayType(DoubleType)), Some(Seq(StringType)))
      }
    }
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="https://en.wikipedia.org/wiki/Word2vec">Word2vec</a>
      |
      | Word2vec is a technique for natural language processing published in 2013.
      | The word2vec algorithm uses a neural network model to learn word associations from a large corpus of text.
      | Once trained, such a model can detect synonymous words or suggest additional words for a partial sentence.
      | As the name implies, word2vec represents each distinct word with a particular list of numbers called a vector.
      | The vectors are chosen carefully such that a simple mathematical function (the cosine similarity between
      | the vectors) indicates the level of semantic similarity between the words represented by those vectors.
      |
      | Use "load modelParams.`Word2VecInPlace-` as output;"
      |
      | to check the available hyper parameters;
      |
      |
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |set rawText='''
      |{"content":"MLSQL是一个好的语言","label":0.0},
      |{"content":"Spark是一个好的语言","label":1.0}
      |{"content":"MLSQL语言","label":0.0}
      |{"content":"MLSQL是一个好的语言","label":0.0}
      |{"content":"MLSQL是一个好的语言","label":1.0}
      |{"content":"MLSQL是一个好的语言","label":0.0}
      |{"content":"MLSQL是一个好的语言","label":0.0}
      |{"content":"MLSQL是一个好的语言","label":1.0}
      |{"content":"Spark好的语言","label":0.0}
      |{"content":"MLSQL是一个好的语言","label":0.0}
      |''';
      |
      |load jsonStr.`rawText` as orginal_text_corpus;
      |
      |train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vec`
      |where inputCol="content"
      |and ignoreNature="true"
      |and resultFeature="merge";
      |
      |;
    """.stripMargin)

}
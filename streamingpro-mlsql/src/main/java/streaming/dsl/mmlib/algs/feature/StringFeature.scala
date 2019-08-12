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

package streaming.dsl.mmlib.algs.feature

import _root_.streaming.dsl.mmlib.algs.MetaConst._
import _root_.streaming.dsl.mmlib.algs._
import _root_.streaming.log.WowLog
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.help.HSQLStringIndex
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, functions => F, _}
import tech.mlsql.common.utils.log.Logging

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}


/**
  * Created by allwefantasy on 14/5/2018.
  */
object StringFeature extends BaseFeatureFunctions with Logging with WowLog {


  def loadStopwords(df: DataFrame, stopWordsPaths: String) = {
    val stopwords = if (stopWordsPaths == null || stopWordsPaths.isEmpty) {
      Set[String]()
    } else {
      val dtt = new SQLDicOrTableToArray()
      val stopwordsMapping = dtt.internal_train(df,
        Map(
          "dic.paths" -> stopWordsPaths,
          "dic.names" -> "stopwords"
        )).collect().map(f => (f.getString(0), f.getSeq(1))).toMap
      stopwordsMapping("stopwords").toSet[String]
    }
    stopwords
  }

  def loadPriorityWords(df: DataFrame, priorityDicPaths: String,
                        priority: Double,
                        predictSingleWordFunc: String => Int) = {
    val priorityWords = (if (priorityDicPaths == null || priorityDicPaths.isEmpty) {
      Set[String]()

    } else {
      val dtt = new SQLDicOrTableToArray()
      val prioritywordsMapping = dtt.internal_train(df,
        Map(
          "dic.paths" -> priorityDicPaths,
          "dic.names" -> "prioritywords"
        )).collect().map(f => (f.getString(0), f.getSeq(1))).toMap
      prioritywordsMapping("prioritywords").toSet[String]
    }).map(f => predictSingleWordFunc(f)).filter(f => f != -1)

    val prioritywordsBr = df.sparkSession.sparkContext.broadcast(priorityWords)

    val priorityFunc = (vec: Vector) => {
      val indices = ArrayBuffer[Int]()
      val values = ArrayBuffer[Double]()
      vec.foreachActive { (index, value) =>
        val newValue = if (prioritywordsBr.value.contains(index)) {
          value * priority
        } else value
        indices += index
        values += newValue
      }
      Vectors.sparse(vec.size, indices.toArray, values.toArray)
    }

    (priorityWords, priorityFunc)
  }


  def analysisWords(df: DataFrame, metaPath: String, dicPaths: String, inputCol: String,
                    stopwordsBr: Broadcast[Set[String]],
                    nGrams: Seq[Int],
                    outputWordAndIndex: Boolean,
                    split: String,
                    wordsArray: Array[String] = Array[String]()
                   ) = {
    val config = Map("dic.paths" -> dicPaths, "inputCol" -> inputCol, "ignoreNature" -> "true", "wordsArray" -> wordsArray.mkString(","), "split" -> split)
    logInfo(format(s"[TFIDF] analysis content with configuration ${config.map(f => s"${f._1}->${f._2}").mkString(" ; ")}"))
    var newDF = new SQLTokenAnalysis().internal_train(df, config)

    logInfo(format(s"[TFIDF] save analysis content to ${ANALYSYS_WORDS_PATH(metaPath, inputCol)}"))
    newDF.write.mode(SaveMode.Overwrite).parquet(ANALYSYS_WORDS_PATH(metaPath, inputCol))
    newDF = newDF.sparkSession.read.parquet(ANALYSYS_WORDS_PATH(metaPath, inputCol))

    val filterStopWordFunc = F.udf((a: Seq[String]) => {
      a.filterNot(stopwordsBr.value.contains(_))
    })
    newDF = replaceColumn(newDF, inputCol, filterStopWordFunc)
    //ngram support
    val ngramfields = nGrams.map { ngram =>
      val newField = inputCol + "_ngrams_" + ngram
      val ngramTF = new NGram().setN(ngram).setInputCol(inputCol).setOutputCol(newField)
      newDF = ngramTF.transform(newDF)
      newField
    }
    val mergeFunc = F.udf((a: Seq[String], b: Seq[String]) => {
      a ++ b
    })

    logInfo(format(s"[TFIDF] building ngram with fields ${ngramfields.mkString(",")}"))
    ngramfields.foreach { newField =>
      newDF = newDF.withColumn(inputCol, mergeFunc(F.col(inputCol), F.col(newField))).drop(newField)
    }

    val inputColIndex = newDF.schema.fieldIndex(inputCol)
    val newRdd = newDF.rdd.flatMap(f =>
      f.getSeq[String](inputColIndex)
    ).distinct().map(f =>
      Row.fromSeq(Seq(f))
    )

    //create uniq int for analysed token
    logInfo(format(s"[TFIDF] preapre to compute how many words in corpus"))
    val tmpWords = df.sparkSession.createDataFrame(newRdd, StructType(Seq(StructField("words", StringType))))
    val wordCount = tmpWords.count()
    logInfo(format(s"[TFIDF] total words in corpus: ${wordCount}"))


    //represent content with sequence of number
    logInfo(format(s"[TFIDF] convert all word to a number"))
    val si = new SQLStringIndex()
    si.train(tmpWords, WORD_INDEX_PATH(metaPath, inputCol), Map("inputCol" -> "words"))


    val siModel = si.load(df.sparkSession, WORD_INDEX_PATH(metaPath, inputCol), Map())

    // keep word and index
    val wordToIndex = HSQLStringIndex.wordToIndex(df.sparkSession, siModel)

    logInfo(format(s"[TFIDF] wordToIndex: ${wordToIndex.size}"))

    logInfo(format(s"[TFIDF] save word to index mapping to ${WORD_INDEX_PATH(metaPath, inputCol)}"))

    val spark = df.sparkSession
    spark.createDataFrame(
      spark.sparkContext.parallelize(wordToIndex.toSeq).map(f => Row.fromSeq(Seq(f._1, f._2))),
      StructType(Seq(
        StructField("word", StringType),
        StructField("index", DoubleType)
      ))).write.
      mode(SaveMode.Overwrite).
      parquet(WORD_INDEX_PATH(metaPath, inputCol))

    if (outputWordAndIndex) {
      val res = wordToIndex.toSeq.sortBy(f => f._2).map(f => s"${f._1}:${f._2}").mkString("\n")
      logInfo(format(res))
    }
    logInfo(format(s"[TFIDF] convert all word to number"))
    val funcMap = si.internal_predict(df.sparkSession, siModel, "wow")
    val predictFunc = funcMap("wow_array").asInstanceOf[(Seq[String]) => Array[Int]]
    val udfPredictFunc = F.udf(predictFunc)
    newDF = replaceColumn(newDF, inputCol, udfPredictFunc)
    (newDF, funcMap, wordCount)
  }

  def tfidf(df: DataFrame,
            metaPath: String,
            dicPaths: String,
            inputCol: String,
            stopWordsPaths: String,
            priorityDicPaths: String,
            priority: Double,
            nGrams: Seq[Int],
            split: String,
            outputWordAndIndex: Boolean = false
           ) = {

    //check stopwords dic is whether configured
    if (stopWordsPaths != null && !stopWordsPaths.isEmpty) {
      logInfo(format(s"[TFIDF] load stopwords from ${stopWordsPaths} "))
    }
    val stopwords = loadStopwords(df, stopWordsPaths)
    val stopwordsBr = df.sparkSession.sparkContext.broadcast(stopwords)

    //analysis
    var (newDF, funcMap, wordCount) = analysisWords(df, metaPath, dicPaths, inputCol, stopwordsBr, nGrams, outputWordAndIndex, split)
    val spark = df.sparkSession

    //tfidf feature
    val tfIDFconfig = Map("inputCol" -> inputCol, "numFeatures" -> wordCount.toString, "binary" -> "false")
    logInfo(format(s"[TFIDF] run tf/idf estimator with follow configuration ${tfIDFconfig.map(s => s"${s._1}->${s._2}").mkString(" ; ")} "))
    val tfidf = new SQLTfIdf()
    tfidf.train(newDF, TF_IDF_PATH(metaPath, inputCol), tfIDFconfig)
    val tfidfModel = tfidf.load(df.sparkSession, TF_IDF_PATH(metaPath, inputCol), Map())
    val tfidfFunc = tfidf.internal_predict(df.sparkSession, tfidfModel, "wow")("wow")
    val tfidfUDFFunc = F.udf(tfidfFunc)
    newDF = replaceColumn(newDF, inputCol, tfidfUDFFunc)

    //enhance
    logInfo(format(s"[TFIDF] enhance priorityWords"))
    val predictSingleWordFunc = funcMap("wow").asInstanceOf[(String) => Int]
    val (priorityWords, priorityFunc) = loadPriorityWords(newDF, priorityDicPaths, priority, predictSingleWordFunc)
    newDF = replaceColumn(newDF, inputCol, F.udf(priorityFunc))

    newDF
  }

  def word2vec(df: DataFrame, metaPath: String, dicPaths: String, wordvecPaths: String, inputCol: String, stopWordsPaths: String, resultFeature: String, split: String, vectorSize: Int = 100, length: Int = 100, minCount: Int = 1) = {
    val stopwords = loadStopwords(df, stopWordsPaths)
    val stopwordsBr = df.sparkSession.sparkContext.broadcast(stopwords)
    val spark = df.sparkSession
    val wordsArray = loadDicsFromWordvec(spark, wordvecPaths)
    val wordvecsMap = loadWordvecs(spark, wordvecPaths)
    if (wordvecsMap.size > 0) {
      var newDF = new SQLTokenAnalysis().internal_train(df, Map("dic.paths" -> dicPaths, "inputCol" -> inputCol, "ignoreNature" -> "true", "wordsArray" -> wordsArray.mkString(",")))
      val filterStopWordFunc = F.udf((a: Seq[String]) => {
        a.filterNot(stopwordsBr.value.contains(_))
      })
      newDF = replaceColumn(newDF, inputCol, filterStopWordFunc)
      val toVecFunc = F.udf((wordArray: Seq[String]) => {
        val r = new Array[Seq[Double]](length)
        val wSize = wordArray.size
        for (i <- 0 until length) {
          if (i < wSize && wordvecsMap.contains(wordArray(i))) {
            r(i) = wordvecsMap(wordArray(i)).toSeq
          } else
            r(i) = new Array[Double](vectorSize).toSeq
        }
        r.toSeq
      })
      replaceColumn(newDF, inputCol, toVecFunc)
    } else {
      var (newDF, funcMap, wordCount) = analysisWords(df, metaPath, dicPaths, inputCol, stopwordsBr, Seq(), false, split, wordsArray)
      // word2vec only accept String sequence, so we should convert int to str
      val word2vec = new SQLWord2Vec()
      word2vec.train(replaceColumn(newDF, inputCol, F.udf((a: Seq[Int]) => {
        a.map(f => f.toString)
      })), WORD2VEC_PATH(metaPath, inputCol), Map("inputCol" -> inputCol, "vectorSize" -> (vectorSize + ""), "minCount" -> minCount.toString))
      val model = word2vec.load(df.sparkSession, WORD2VEC_PATH(metaPath, inputCol), Map())
      if (!resultFeature.equals("index")) {
        val predictFunc = word2vec.internal_predict(df.sparkSession, model, "wow")("wow_array").asInstanceOf[(Seq[String]) => Seq[Seq[Double]]]
        val udfPredictFunc = F.udf(predictFunc)
        newDF = replaceColumn(newDF, inputCol, udfPredictFunc)
      }
      newDF
    }
  }

  def loadDicsFromWordvec(spark: SparkSession, wordvecPaths: String) = {
    var result = Array[String]()
    result ++= wordvecPaths.split(",").filter(f => !f.isEmpty).flatMap { f =>
      spark.sparkContext.textFile(f).map(line =>
        line.split(" ")(0)
      ).collect()
    }
    result
  }

  def loadWordvecs(spark: SparkSession, wordvecPaths: String) = {
    var wordVecMap = Map[String, Array[Double]]()
    var wordVec = Array[(String, Array[Double])]()
    wordVec ++= wordvecPaths.split(",").filter(f => !f.isEmpty).flatMap { f =>
      spark.sparkContext.textFile(f).map(line =>
        (line.split(" ")(0), line.split(" ")(1).split(",").map(a => a.toDouble))
      ).collect()
    }
    for (a <- wordVec) {
      wordVecMap += a
    }
    wordVecMap
  }

  def wordvec(df: DataFrame, dicPaths: String, inputCol: String, stopWordsPaths: String, vectorSize: Int, length: Int) = {
    val spark = df.sparkSession
    val stopwords = loadStopwords(df, stopWordsPaths)
    val stopwordsBr = df.sparkSession.sparkContext.broadcast(stopwords)
    var wordVecMap = Map[String, Array[Double]]()
    val wordVec = dicPaths.split(",").filter(f => !f.isEmpty).flatMap { f =>
      spark.sparkContext.textFile(f).map(line =>
        (line.split(" ")(0), line.split(" ")(1).split(",").map(a => a.toDouble))
      ).collect()
    }
    val dic = wordVec.map(_._1)
    for (a <- wordVec) {
      wordVecMap += a
    }
    val rdd = df.rdd.mapPartitions { mp =>
      val parser = SQLTokenAnalysis.createAnalyzer(dic, Map())
      mp.map { f =>
        val content = f.getAs[String](inputCol)
        val res = SQLTokenAnalysis.parseStr(parser, content, Map())
        val index = f.fieldIndex(inputCol)
        val newValue = f.toSeq.zipWithIndex.filterNot(f => f._2 == index).map(f => f._1) ++ Seq(res)
        Row.fromSeq(newValue)
      }
    }
    var newDF = spark.createDataFrame(rdd,
      StructType(df.schema.filterNot(f => f.name == inputCol) ++ Seq(StructField(inputCol, ArrayType(StringType)))))
    val filterStopWordFunc = F.udf((a: Seq[String]) => {
      a.filterNot(stopwordsBr.value.contains(_))
    })
    newDF = replaceColumn(newDF, inputCol, filterStopWordFunc)

    //toVec
    val toVecFunc = F.udf((wordArray: Seq[String]) => {
      val r = new Array[Seq[Double]](length)
      val wSize = wordArray.size
      for (i <- 0 until length) {
        if (i < wSize && wordVecMap.contains(wordArray(i))) {
          r(i) = wordVecMap(wordArray(i)).toSeq
        } else
          r(i) = new Array[Double](vectorSize).toSeq
      }
      r.toSeq
    })
    newDF = replaceColumn(newDF, inputCol, toVecFunc)
    newDF
  }

  def strToInt(df: DataFrame, mappingPath: String, inputCol: String, outputWordAndIndex: Boolean) = {
    val wordIndexPath = mappingPath.stripSuffix("/") + s"/wordIndex/$inputCol"
    val si = new SQLStringIndex()
    si.train(df, wordIndexPath, Map("inputCol" -> inputCol))
    val siModel = si.load(df.sparkSession, wordIndexPath, Map())

    if (outputWordAndIndex) {
      val wordToIndex = HSQLStringIndex.wordToIndex(df.sparkSession, siModel)
      val res = wordToIndex.toSeq.sortBy(f => f._2).map(f => s"${f._1}:${f._2}").mkString("\n")
      println(res)
    }
    val funcMap = si.internal_predict(df.sparkSession, siModel, "wow")
    val predictSingleWordFunc = funcMap("wow").asInstanceOf[(String) => Int]
    val newDF = replaceColumn(df, inputCol, F.udf(predictSingleWordFunc))
    (newDF, funcMap)
  }

  def mergeFunc(seq: Seq[Seq[Double]], vectorSize: Int) = {
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

  def analysisRaw(df: DataFrame, inputCol: String, sentenceSplit: String, modelSplit: String, dicPaths: String, wordsArray: Array[String] = Array[String]()) = {
    val spark = df.sparkSession
    val words = SQLTokenAnalysis.loadDics(spark, Map("dic.paths" -> dicPaths)) ++ wordsArray
    val parser = SQLTokenAnalysis.createAnalyzer(words, Map())
    val analysisRawFunc = F.udf((raw: String) => {
      var raw1 = ""
      val splitArray = sentenceSplit.split("")
      for (split <- splitArray) {
        raw1 = raw.replace(split, "。")
      }
      raw1.split("。").map(f => {
        if (modelSplit != null) {
          f.split(modelSplit).toSeq
        } else {
          val parser = SQLTokenAnalysis.createAnalyzer(words, Map())
          SQLTokenAnalysis.parseStr(parser, f, Map("ignoreNature" -> "true")).toSeq
        }
      }).toSeq
    })
    replaceColumn(df, inputCol, analysisRawFunc)
  }

  def raw2vec(df: DataFrame, inputCol: String, sentenceSplit: String, modelPath: String) = {
    val spark = df.sparkSession
    import spark.implicits._
    val modelMetaPath = getMetaPath(modelPath)
    val modelParams = spark.read.parquet(PARAMS_PATH(modelMetaPath, "params")).map(f => (f.getString(0), f.getString(1))).collect().toMap
    val modelInputCol = modelParams.getOrElse("inputCol", "")
    val dicPaths = modelParams.getOrElse("dicPaths", "")
    val wordvecPaths = modelParams.getOrElse("wordvecPaths", "")
    val modelSplit = modelParams.getOrElse("split", null)
    val vectorSize = modelParams.getOrElse("vectorSize", "100").toInt
    val wordvecsMapBr = spark.sparkContext.broadcast(loadWordvecs(spark, wordvecPaths))
    val wordsArray = StringFeature.loadDicsFromWordvec(spark, wordvecPaths)
    val word2vec = new SQLWord2Vec()
    val model = word2vec.load(spark, WORD2VEC_PATH(modelMetaPath, modelInputCol), Map())
    val predictFunc = word2vec.internal_predict(df.sparkSession, model, "wow")("wow_array").asInstanceOf[(Seq[String]) => Seq[Seq[Double]]]
    val wordIndexBr = spark.sparkContext.broadcast(spark.read.parquet(WORD_INDEX_PATH(modelMetaPath, modelInputCol)).map(f => ((f.getString(0), f.getDouble(1)))).collect().toMap)

    val newDf = analysisRaw(df, inputCol, sentenceSplit, modelSplit, dicPaths, wordsArray)
    val toVecFunc = F.udf((raw: Seq[Seq[String]]) => {
      raw.map(wordSeq => {
        val vecSeq = if (wordvecsMapBr.value.size > 0) {
          wordSeq.filter(f => wordvecsMapBr.value.contains(f)).map(f => wordvecsMapBr.value(f).toSeq)
        }
        else {
          val wordIntArray = wordSeq.filter(f => wordIndexBr.value.contains(f)).map(f => wordIndexBr.value(f).toInt)
          predictFunc(wordIntArray.map(f => f.toString))
        }
        //merge
        if (vecSeq.size == 0) {
          Seq[Double]()
        }
        else {
          val r = new Array[Double](vectorSize)
          for (vec <- vecSeq) {
            for (i <- 0 until vec.size) {
              r(i) = vec(i) + r(i)
            }
          }
          r.toSeq
        }
      })
    })
    replaceColumn(newDf, inputCol, toVecFunc)
  }

  def cosineSimilarity(array1: Seq[Double], array2: Seq[Double]): Double = {
    var dotProduct = 0.0
    var magnitude1 = 0.0
    var magnitude2 = 0.0
    var cosineSimilarity = 0.0
    if (array1.length == 0 || array2.length == 0 || array1.length != array2.length) {
      0.0D
    } else {
      for (i <- 0 until array1.length) {
        dotProduct += array1(i) * array2(i) //a.b
        magnitude1 += Math.pow(array1(i), 2) //(a^2)
        magnitude2 += Math.pow(array2(i), 2) //(b^2)
      }
      magnitude1 = Math.sqrt(magnitude1) //sqrt(a^2)
      magnitude2 = Math.sqrt(magnitude2) //sqrt(b^2)
      if ((magnitude1 != 0.0D) && (magnitude2 != 0.0D))
        cosineSimilarity = dotProduct / (magnitude1 * magnitude2)
      else return 0.0
      cosineSimilarity
    }
  }

  def rawSimilar(array1: Seq[Seq[Double]], array2: Seq[Seq[Double]], threshold: Double = 0.8): Double = {
    var similar: Double = 0
    if (array1.length > 0 && array2.length > 0) {
      val (smallArray, bigArray) = {
        if (array1.length < array2.length)
          (array1, array2)
        else
          (array2, array1)
      }
      val size = smallArray.length.toDouble
      var num = 0
      for (r1 <- smallArray) {
        breakable {
          for (r2 <- bigArray) {
            if (cosineSimilarity(r1, r2) >= threshold) {
              num = num + 1
              break
            }
          }
        }
      }
      similar = num / size
    }
    similar
  }
}

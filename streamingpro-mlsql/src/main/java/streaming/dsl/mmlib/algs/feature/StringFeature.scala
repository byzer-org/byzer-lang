package streaming.dsl.mmlib.algs.feature

import _root_.streaming.dsl.mmlib.algs._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.NGram
import org.apache.spark.ml.help.HSQLStringIndex
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, functions => F, _}
import _root_.streaming.dsl.mmlib.algs.MetaConst._

import scala.collection.mutable.ArrayBuffer


/**
 * Created by allwefantasy on 14/5/2018.
 */
object StringFeature extends BaseFeatureFunctions {


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
    var newDF = new SQLTokenAnalysis().internal_train(df, Map("dic.paths" -> dicPaths, "inputCol" -> inputCol, "ignoreNature" -> "true", "wordsArray" -> wordsArray.mkString(","), "split" -> split))
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
    val tmpWords = df.sparkSession.createDataFrame(newRdd, StructType(Seq(StructField("words", StringType))))
    val wordCount = tmpWords.count()

    //represent content with sequence of number
    val si = new SQLStringIndex()
    si.train(tmpWords, WORD_INDEX_PATH(metaPath, inputCol), Map("inputCol" -> "words"))


    val siModel = si.load(df.sparkSession, WORD_INDEX_PATH(metaPath, inputCol), Map())

    // keep word and index
    val wordToIndex = HSQLStringIndex.wordToIndex(df.sparkSession, siModel)

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
      println(res)
    }

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

    val stopwords = loadStopwords(df, stopWordsPaths)
    val stopwordsBr = df.sparkSession.sparkContext.broadcast(stopwords)

    //analysis
    var (newDF, funcMap, wordCount) = analysisWords(df, metaPath, dicPaths, inputCol, stopwordsBr, nGrams, outputWordAndIndex, split)
    val spark = df.sparkSession

    //tfidf feature
    val tfidf = new SQLTfIdf()
    tfidf.train(newDF, TF_IDF_PATH(metaPath, inputCol), Map("inputCol" -> inputCol, "numFeatures" -> wordCount.toString, "binary" -> "true"))
    val tfidfModel = tfidf.load(df.sparkSession, TF_IDF_PATH(metaPath, inputCol), Map())
    val tfidfFunc = tfidf.internal_predict(df.sparkSession, tfidfModel, "wow")("wow")
    val tfidfUDFFunc = F.udf(tfidfFunc)
    newDF = replaceColumn(newDF, inputCol, tfidfUDFFunc)

    //enhance
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
}

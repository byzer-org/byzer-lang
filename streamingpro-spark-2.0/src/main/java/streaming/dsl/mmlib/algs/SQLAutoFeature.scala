package streaming.dsl.mmlib.algs

import org.apache.spark.ml.help.HSQLStringIndex
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 2/5/2018.
  */
class SQLAutoFeature extends SQLAlg with Functions {
  //val path = "/tmp/" + UUID.randomUUID().toString
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}

object StringFeature {

  private def replaceColumn(newDF: DataFrame, inputCol: String, udf: UserDefinedFunction) = {
    newDF.withColumn(inputCol + "_tmp", udf(F.col(inputCol))).drop(inputCol).withColumnRenamed(inputCol + "_tmp", inputCol)
  }

  private def loadStopwords(df: DataFrame, stopWordsPaths: String) = {
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

  private def loadPriorityWords(df: DataFrame, priorityDicPaths: String, predictSingleWordFunc: String => Int) = {
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
    priorityWords
  }

  def tfidf(df: DataFrame,
            mappingPath: String,
            dicPaths: String,
            inputCol: String,
            stopWordsPaths: String,
            priorityDicPaths: String,
            priority: Double,
            outputWordAndIndex: Boolean = false
           ) = {

    //check stopwords dic is whether configured

    val stopwords = loadStopwords(df, stopWordsPaths)
    val stopwordsBr = df.sparkSession.sparkContext.broadcast(stopwords)

    //analysis
    var newDF = new SQLTokenAnalysis().internal_train(df, Map("dic.paths" -> dicPaths, "inputCol" -> inputCol, "ignoreNature" -> "true"))
    val inputColIndex = newDF.schema.fieldIndex(inputCol)
    val newRdd = newDF.rdd.flatMap(f =>
      f.getSeq[String](inputColIndex)
    ).filter(f => !stopwordsBr.value.contains(f)).distinct().map(f =>
      Row.fromSeq(Seq(f))
    )

    //create uniq int for analysed token
    val tmpWords = df.sparkSession.createDataFrame(newRdd, StructType(Seq(StructField("words", StringType))))
    val wordCount = tmpWords.count()

    //represent content with sequence of number
    val wordIndexPath = mappingPath.stripSuffix("/") + s"/wordIndex/$inputCol"
    val si = new SQLStringIndex()
    si.train(tmpWords, wordIndexPath, Map("inputCol" -> "words"))


    val siModel = si.load(df.sparkSession, wordIndexPath, Map())

    if (outputWordAndIndex) {
      val wordToIndex = HSQLStringIndex.wordToIndex(df.sparkSession, siModel)
      val res = wordToIndex.toSeq.sortBy(f => f._2).map(f => s"${f._1}:${f._2}").mkString("\n")
      println(res)
    }


    val funcMap = si.internal_predict(df.sparkSession, siModel, "wow")
    val predictFunc = funcMap("wow_array").asInstanceOf[(Seq[String]) => Array[Int]]
    val udfPredictFunc = F.udf(predictFunc)
    newDF = replaceColumn(newDF, inputCol, udfPredictFunc)


    val predictSingleWordFunc = funcMap("wow").asInstanceOf[(String) => Int]
    val priorityWords = loadPriorityWords(newDF,priorityDicPaths,predictSingleWordFunc)
    val prioritywordsBr = df.sparkSession.sparkContext.broadcast[Set[Int]](priorityWords)


    //tfidf feature
    val tfidfPath = mappingPath.stripSuffix("/") + s"/tfidf/$inputCol"
    val tfidf = new SQLTfIdf()
    tfidf.train(newDF, tfidfPath, Map("inputCol" -> inputCol, "numFeatures" -> wordCount.toString, "binary" -> "true"))
    val tfidfModel = tfidf.load(df.sparkSession, tfidfPath, Map())
    val tfidfFunc = tfidf.internal_predict(df.sparkSession, tfidfModel, "wow")("wow")
    val tfidfUDFFunc = F.udf(tfidfFunc)
    newDF = replaceColumn(newDF, inputCol, tfidfUDFFunc)

    // enhance priority word weight
    val priorityFunc = F.udf((vec: Vector) => {
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
    })

    newDF = replaceColumn(newDF, inputCol, priorityFunc)
    newDF
  }

  def word2vec(df: DataFrame, mappingPath: String, dicPaths: String, inputCol: String) = {
    var newDF = new SQLTokenAnalysis().internal_train(df, Map("dic.paths" -> dicPaths, "inputCol" -> inputCol))
    val word2vec = new SQLWord2Vec()
    val word2vecPath = mappingPath.stripSuffix("/") + s"/word2vec/$inputCol"
    word2vec.train(newDF, word2vecPath, Map("inputCol" -> inputCol, "minCount" -> "0"))
    val model = word2vec.load(df.sparkSession, word2vecPath, Map())
    val predictFunc = word2vec.internal_predict(df.sparkSession, model, "wow")("wow_array").asInstanceOf[(Seq[String]) => Seq[Seq[Double]]]
    val udfPredictFunc = F.udf(predictFunc)
    newDF = replaceColumn(newDF, inputCol, udfPredictFunc)
    newDF
  }
}

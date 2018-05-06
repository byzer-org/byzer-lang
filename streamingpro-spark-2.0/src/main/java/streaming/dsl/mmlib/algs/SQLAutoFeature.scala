package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{functions => F}

/**
  * Created by allwefantasy on 2/5/2018.
  */
class SQLAutoFeature extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    null
  }
}

object StringFeature {

  private def replaceColumn(newDF: DataFrame, inputCol: String, udf: UserDefinedFunction) = {
    newDF.withColumn(inputCol + "_tmp", udf(F.col(inputCol))).drop(inputCol).withColumnRenamed(inputCol + "_tmp", inputCol)
  }

  //  private def fetchPredictFun(newDF: DataFrame,
  //                              mappingPath: String,
  //                              inputCol: String,
  //                              funcName: String
  //                             ): UserDefinedFunction = {
  //    val wordIndexPath = mappingPath.stripSuffix("/") + s"/wordIndex/$inputCol"
  //    val si = new SQLStringIndex()
  //    si.train(newDF, wordIndexPath, Map("inputCol" -> "words"))
  //    val siModel = si.load(newDF.sparkSession, wordIndexPath)
  //    val predictFunc = si.internal_predict(newDF.sparkSession, siModel, "wow")(funcName).asInstanceOf[(Seq[String]) => Array[Int]]
  //    F.udf(predictFunc)
  //  }

  //val path = "/tmp/" + UUID.randomUUID().toString
  def tfidf(df: DataFrame, mappingPath: String, dicPaths: String, inputCol: String) = {
    //analysis
    var newDF = new SQLTokenAnalysis().internal_train(df, Map("dic.paths" -> dicPaths, "inputCol" -> inputCol))
    val inputColIndex = newDF.schema.fieldIndex(inputCol)
    val newRdd = newDF.rdd.flatMap(f =>
      f.getSeq[String](inputColIndex)
    ).distinct().map(f =>
      Row.fromSeq(Seq(f))
    )

    //create uniq int for analysed token
    println(newRdd.collect())
    val tmpWords = df.sparkSession.createDataFrame(newRdd, StructType(Seq(StructField("words", StringType))))
    val wordCount = tmpWords.count()

    //represent content with sequence of number
    val wordIndexPath = mappingPath.stripSuffix("/") + s"/wordIndex/$inputCol"
    val si = new SQLStringIndex()
    si.train(tmpWords, wordIndexPath, Map("inputCol" -> "words"))
    val siModel = si.load(df.sparkSession, wordIndexPath)

    val predictFunc = si.internal_predict(df.sparkSession, siModel, "wow")("wow_array").asInstanceOf[(Seq[String]) => Array[Int]]
    val udfPredictFunc = F.udf(predictFunc)
    newDF = replaceColumn(newDF, inputCol, udfPredictFunc)

    //tfidf feature
    val tfidfPath = mappingPath.stripSuffix("/") + s"/tfidf/$inputCol"
    val tfidf = new SQLTfIdf()
    tfidf.train(newDF, tfidfPath, Map("inputCol" -> inputCol, "numFeatures" -> wordCount.toString, "binary" -> "true"))
    val tfidfModel = tfidf.load(df.sparkSession, tfidfPath)
    val tfidfFunc = tfidf.internal_predict(df.sparkSession, tfidfModel, "wow")("wow")
    val tfidfUDFFunc = F.udf(tfidfFunc)
    newDF = replaceColumn(newDF, inputCol, tfidfUDFFunc)
    newDF
  }

  def word2vec(str: String) = {

  }
}

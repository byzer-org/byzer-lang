package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.feature.StringFeature
import MetaConst._
import org.apache.spark.ml.linalg.SQLDataTypes._
import streaming.core.shared.SharedObjManager
import streaming.dsl.mmlib.algs.meta.TFIDFMeta

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 7/5/2018.
  */
class SQLTfIdfInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    interval_train(df, params + ("path" -> path)).write.mode(SaveMode.Overwrite).parquet(getDataPath(path))
  }

  def interval_train(df: DataFrame, params: Map[String, String]) = {
    val dicPaths = params.getOrElse("dicPaths", "")
    val inputCol = params.getOrElse("inputCol", "")
    val stopWordPath = params.getOrElse("stopWordPath", "")
    val priorityDicPath = params.getOrElse("priorityDicPath", "")
    val priority = params.getOrElse("priority", "1").toDouble
    val nGrams = params.getOrElse("nGrams", "").split(",").filterNot(f => f.isEmpty).map(f => f.toInt).toSeq
    require(!inputCol.isEmpty, "inputCol is required when use SQLTfIdfInPlace")
    val path = params("path")

    val metaPath = getMetaPath(path)

    // keep params
    val spark = df.sparkSession
    spark.createDataFrame(
      spark.sparkContext.parallelize(params.toSeq).map(f => Row.fromSeq(Seq(f._1, f._2))),
      StructType(Seq(
        StructField("key", StringType),
        StructField("value", StringType)
      ))).write.
      mode(SaveMode.Overwrite).
      parquet(PARAMS_PATH(metaPath, "params"))

    val newDF = StringFeature.tfidf(df, metaPath, dicPaths, inputCol, stopWordPath, priorityDicPath, priority, nGrams)
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
    UserDefinedFunction(func, VectorType, Some(Seq(StringType)))
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

    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
    val stopwords = StringFeature.loadStopwords(df, stopWordPath)
    val stopwordsBr = spark.sparkContext.broadcast(stopwords)


    val (priorityWords, priorityFunc) = StringFeature.loadPriorityWords(df, priorityDicPath, priority, (str: String) => {
      wordIndexBr.value.getOrElse(str, -1d).toInt
    })

    val ngram = (words: Seq[String], n: Int) => {
      words.iterator.sliding(n).withPartial(false).map(_.mkString(" ")).toSeq
    }

    val func = (content: String) => {

      // create analyser
      val forest = SharedObjManager.getOrCreate[Any](dicPaths, SharedObjManager.forestPool, () => {
        val words = SQLTokenAnalysis.loadDics(spark, trainParams)
        SQLTokenAnalysis.createForest(words, trainParams)
      })
      val parser = SQLTokenAnalysis.createAnalyzerFromForest(forest.asInstanceOf[AnyRef], trainParams)
      // analyser content
      val wordArray = SQLTokenAnalysis.parseStr(parser, content, trainParams).
        filter(f => !stopwordsBr.value.contains(f))

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

}

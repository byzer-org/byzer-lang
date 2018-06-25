package streaming.dsl.mmlib.algs

import MetaConst._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions => F}
import streaming.core.shared.SharedObjManager
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.feature.StringFeature
import streaming.dsl.mmlib.algs.meta.Word2VecMeta

/**
 * Created by allwefantasy on 7/5/2018.
 */
class SQLWord2VecInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    interval_train(df, params + ("path" -> path)).write.mode(SaveMode.Overwrite).parquet(getDataPath(path))
  }

  def interval_train(df: DataFrame, params: Map[String, String]) = {
    val dicPaths = params.getOrElse("dicPaths", "")
    val inputCol = params.getOrElse("inputCol", "")
    val vectorSize = params.getOrElse("vectorSize", "100").toInt
    val stopWordPath = params.getOrElse("stopWordPath", "")
    val flat = params.getOrElse("flatFeature", "") //flat,merge
    require(!inputCol.isEmpty, "inputCol is required when use SQLWord2VecInPlace")
    val metaPath = getMetaPath(params("path"))
    // keep params
    saveTraningParams(df.sparkSession, params, metaPath)

    var newDF = StringFeature.word2vec(df, metaPath, dicPaths, inputCol, stopWordPath, vectorSize)
    if (flat.equals("flat")) {
      val flatFeatureUdf = F.udf((a: Seq[Seq[Double]]) => {
        a.flatten
      })
      newDF = newDF.withColumn(inputCol, flatFeatureUdf(F.col(inputCol)))
    }
    if (flat.equals("merge")) {
      val flatFeatureUdf = F.udf((a: Seq[Seq[Double]]) => {
        val r = new Array[Double](vectorSize)
        for (a1 <- a) {
          val b = a1.toList
          for (i <- 0 until b.size) {
            r(i) = b(i) + r(i)
          }
        }
        r.toSeq
      })
      newDF = newDF.withColumn(inputCol, flatFeatureUdf(F.col(inputCol)))
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
    //load wordindex
    val wordIndex = spark.read.parquet(WORD_INDEX_PATH(path, inputCol)).map(f => ((f.getString(0), f.getDouble(1)))).collect().toMap
    //load word2vec model
    val word2vec = new SQLWord2Vec()
    val model = word2vec.load(spark, WORD2VEC_PATH(path, inputCol), Map())
    val predictFunc = word2vec.internal_predict(df.sparkSession, model, "wow")("wow_array").asInstanceOf[(Seq[String]) => Seq[Seq[Double]]]
    Word2VecMeta(trainParams, wordIndex, predictFunc)
  }

  override def predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val word2vecMeta = _model.asInstanceOf[Word2VecMeta]
    val trainParams = word2vecMeta.trainParams
    val dicPaths = trainParams.getOrElse("dicPaths", "")
    val stopWordPath = trainParams.getOrElse("stopWordPath", "")

    val wordIndexBr = spark.sparkContext.broadcast(word2vecMeta.wordIndex)


    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq()))
    val stopwords = StringFeature.loadStopwords(df, stopWordPath)
    val stopwordsBr = spark.sparkContext.broadcast(stopwords)

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

      val wordIntArray = wordArray.filter(f => wordIndexBr.value.contains(f)).map(f => wordIndexBr.value(f).toInt)
      wordIntArray.map(f => f.toString).toSeq
    }

    if (trainParams.getOrElse("flatFeature", "false").toBoolean) {
      val f2 = (a: String) => {
        word2vecMeta.predictFunc(func(a)).flatMap(f => f)
      }
      UserDefinedFunction(f2, ArrayType(DoubleType), Some(Seq(StringType)))
    } else {
      val f2 = (a: String) => {
        word2vecMeta.predictFunc(func(a))
      }
      UserDefinedFunction(f2, ArrayType(ArrayType(DoubleType)), Some(Seq(StringType)))
    }

  }

}
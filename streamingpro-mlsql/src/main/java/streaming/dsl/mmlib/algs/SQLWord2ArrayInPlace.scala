package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.core.shared.SharedObjManager
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.feature.StringFeature
import streaming.dsl.mmlib.algs.feature.StringFeature.loadWordvecs
import streaming.dsl.mmlib.algs.meta.Word2VecMeta

/**
 * Created by zhuml on 8/8/2018.
 */
class SQLWord2ArrayInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
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
      val wordVec = {
        if (wordVecsBr.value.size > 0)
          wordVecsBr.value
        else
          wordIndexBr.value
      }
      wordArray.filter(f => wordVec.contains(f))
    }
    UserDefinedFunction(func, ArrayType(StringType), Some(Seq(StringType)))
  }
}
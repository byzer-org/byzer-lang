package streaming.core

import net.sf.json.JSONObject
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.BasicSparkOperation
import org.apache.spark.sql.{functions => F}
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.mmlib.algs.StringFeature
import org.apache.spark.ml.linalg.Vector
import streaming.dsl.ScriptSQLExec


/**
  * Created by allwefantasy on 6/5/2018.
  */
class AutoMLSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "tfidf featurize" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq("我是天才，你呢", "你真的很棒", "天才你好")).map { f =>
        Row.fromSeq(Seq(f))
      }
      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(StructField("content", StringType))))
      val newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content")
      val res = newDF.collect()
      assume(res.size == 3)
      assume(res(0).getAs[Vector]("content").size == 10)
      println(newDF.toJSON.collect().mkString("\n"))

    }
  }

  "word2vec featurize" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq("我是天才，你呢", "你真的很棒", "天才你好")).map { f =>
        Row.fromSeq(Seq(f))
      }
      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(StructField("content", StringType))))
      val newDF = StringFeature.word2vec(df, "/tmp/word2vec/mapping", "", "content")
      println(newDF.toJSON.collect().mkString("\n"))

    }
  }

  "sklearn-multi-model" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(scriptStr("sklearn-multi-model-trainning"), sq)
      spark.read.parquet("/tmp/william/tmp/model/0").show()

    }
  }

  "tensorflow-cnn-model" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(scriptStr("tensorflow-cnn"), sq)

    }
  }
}

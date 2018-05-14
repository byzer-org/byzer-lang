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

      writeStringToFile("/tmp/tfidf/stopwords", List("你").mkString("\n"))
      writeStringToFile("/tmp/tfidf/prioritywords", List("天才").mkString("\n"))

      /*
          真的:0.0
          ，:1.0
          棒:2.0
          天才:3.0
          是:4.0
          我:5.0
          很:6.0
          你好:7.0
          呢:8.0
       */
      var newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content", "/tmp/tfidf/stopwords", "/tmp/tfidf/prioritywords", 100000.0, true)
      var res = newDF.collect()
      assume(res.size == 3)
      assume(res(0).getAs[Vector]("content").size == 9)
      var res2 = newDF.collect().filter { f =>
        val v = f.getAs[Vector](f.fieldIndex("content"))
        if (v(3) != 0) {
          assume(v.argmax == 3)
        }
        v(3) != 0
      }
      assume(res2.size == 2)
      println(newDF.toJSON.collect().mkString("\n"))

      newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content", "", null, 100000.0, true)
      res = newDF.collect()
      assume(res(0).getAs[Vector]("content").size == 10)

      res2 = newDF.collect().filter { f =>
        val v = f.getAs[Vector](f.fieldIndex("content"))
        v(v.argmax) < 1
      }
      assume(res2.size == 3)


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

  "sklearn-multi-model-with-sample" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(scriptStr("sklearn-multi-model-trainning-with-sample"), sq)
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

  "SQLSampler" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(scriptStr("sql-sampler"), sq)
      var df = spark.sql("select count(*) as num,__split__ as rate from sample_data group by __split__ ")
      assume(df.count() == 3)
      df = spark.sql("select label,__split__,count(__split__) as rate from sample_data  group by label,__split__ order by label,__split__,rate")
      df.show(10000)

    }
  }
}

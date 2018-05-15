package streaming.core

import java.io.File

import net.sf.json.JSONObject
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.BasicSparkOperation
import org.apache.spark.sql.{functions => F}
import streaming.core.strategy.platform.SparkRuntime
import org.apache.spark.ml.linalg.Vector
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.feature.StringFeature


/**
  * Created by allwefantasy on 6/5/2018.
  */
class AutoMLSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  copySampleLibsvmData

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
      var newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content", "/tmp/tfidf/stopwords", "/tmp/tfidf/prioritywords", 100000.0, Seq(), true)
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

      newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content", "", null, 100000.0, Seq(), true)
      res = newDF.collect()
      assume(res(0).getAs[Vector]("content").size == 10)

      res2 = newDF.collect().filter { f =>
        val v = f.getAs[Vector](f.fieldIndex("content"))
        v(v.argmax) < 1
      }
      assume(res2.size == 3)


    }
  }

  "tfidf featurize with ngram" should "work fine" in {

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
          我 是 天才:1.0
          我 是:2.0
          你:3.0
          天才 ， 你:4.0
          天才 ，:5.0
          ， 你 呢:6.0
          你 真的 很:7.0
          ，:8.0
          真的 很:9.0
          棒:10.0
          ， 你:11.0
          很 棒:12.0
          是 天才:13.0
          你 呢:14.0
          天才 你好:15.0
          天才:16.0
          是:17.0
          你 真的:18.0
          我:19.0
          很:20.0
          是 天才 ，:21.0
          你好:22.0
          真的 很 棒:23.0
          呢:24.0
       */

      val newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content", "", null, 100000.0, Seq(2, 3), true)
      val res = newDF.collect()
      assume(res(0).getAs[Vector]("content").size == 25)
      newDF.show(false)
    }
  }

  "test" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      import org.apache.spark.ml.feature.NGram

      val wordDataFrame = spark.createDataFrame(Seq(
        (0, Array("Hi", "I", "heard", "about", "Spark")),
        (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
        (2, Array("Logistic", "regression", "models", "are", "neat"))
      )).toDF("id", "words")

      val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

      val ngramDataFrame = ngram.transform(wordDataFrame)
      ngramDataFrame.select("ngrams").show(false)

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
      val newDF = StringFeature.word2vec(df, "/tmp/word2vec/mapping", "", "content", null)
      println(newDF.toJSON.collect().mkString("\n"))

    }
  }

  def copySampleLibsvmData = {
    writeStringToFile("/tmp/william/sample_libsvm_data.txt", loadDataStr("sample_libsvm_data.txt"))
  }

  "sklearn-multi-model" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("sklearn-multi-model-trainning"), sq)
      spark.read.parquet("/tmp/william/tmp/model/0").show()
    }
  }

  "sklearn-multi-model-with-sample" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("sklearn-multi-model-trainning-with-sample"), sq)
      spark.read.parquet("/tmp/william/tmp/model/0").show()
    }
  }

  "tensorflow-cnn-model" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("tensorflow-cnn"), sq)

    }
  }

  "SQLSampler" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("sql-sampler"), sq)
      var df = spark.sql("select count(*) as num,__split__ as rate from sample_data group by __split__ ")
      assume(df.count() == 3)
      df = spark.sql("select label,__split__,count(__split__) as rate from sample_data  group by label,__split__ order by label,__split__,rate")
      df.show(10000)

    }
  }
}

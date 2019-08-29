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

package streaming.test.processing

import net.sf.json.JSONObject
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.shared.SharedObjManager
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.feature.StringFeature
import streaming.dsl.mmlib.algs.{SQLCommunityBasedSimilarityInPlace, SQLCorpusExplainInPlace, SQLVecMapInPlace}
import streaming.dsl.template.TemplateMerge


/**
  * Created by allwefantasy on 6/5/2018.
  */
class TextSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  val isAnjsAnalyzerLoaded = () => {
    try {
      Class.forName("org.ansj.splitWord.analysis.NlpAnalysis")
      true
    } catch {
      case e: Exception =>
        false
    }
  }

  def analyzerContext[R](block: () => R): R = {
    if (isAnjsAnalyzerLoaded()) {
      block()
    } else assume(1 == 1).asInstanceOf[R]
  }

  "tfidf featurize" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql

      analyzerContext(() => {
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
        var newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content", "/tmp/tfidf/stopwords", "/tmp/tfidf/prioritywords", 100000.0, Seq(), null, true)
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

        newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content", "", null, 100000.0, Seq(), null, true)
        res = newDF.collect()
        assume(res(0).getAs[Vector]("content").size == 10)

        res2 = newDF.collect().filter { f =>
          val v = f.getAs[Vector](f.fieldIndex("content"))
          v(v.argmax) < 1
        }
        assume(res2.size == 3)
      })


    }
  }

  "tfidf featurize with ngram" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      analyzerContext(() => {
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

        val newDF = StringFeature.tfidf(df, "/tmp/tfidf/mapping", "", "content", "", null, 100000.0, Seq(2, 3), null, true)
        val res = newDF.collect()
        assume(res(0).getAs[Vector]("content").size == 25)
        newDF.show(false)
      })

    }
  }


  "word2vec featurize" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      analyzerContext(() => {
        //执行sql
        implicit val spark = runtime.sparkSession
        val dataRDD = spark.sparkContext.parallelize(Seq("我是天才，你呢", "你真的很棒", "天才你好")).map { f =>
          Row.fromSeq(Seq(f))
        }
        val df = spark.createDataFrame(dataRDD,
          StructType(Seq(StructField("content", StringType))))
        val newDF = StringFeature.word2vec(df, "/tmp/word2vec/mapping", "", "", "content", null, "", null)
        println(newDF.toJSON.collect().mkString("\n"))
      })


    }
  }

  "SQLFeatureExtractInPlace" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq("请联系 13634282910", "dog"),
        Seq("扣扣 527153688@qq.com", "cat"),
        Seq("www.baidu.com www.baidu.com www.baidu.com", "cat"),
        Seq("热线来电   12222222", "rabbit"),
        Seq("img.dxycdn.com豆腐豆腐   img.dxycdn.com  ddfd img.dxycdn.com", "cat"),
        Seq("<html> dddd img.dxycdn.com ffff 527153688@qq.com ", "cat"),
        Seq("dog,rabbit.我  在  这 我 我 我 里123,22", "dog"))).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("doc", StringType),
          StructField("predict", StringType)
        )))

      df.createOrReplaceTempView("FeatureExtractInPlaceData")

      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("feature-extract"), sq)

      spark.sql("select * from parquet.`/tmp/william/tmp/featureExtractInPlace/data`").show(10, false)

      assume(spark.sql(
        """select mpredict_email('扣扣 527153688@qq.com') as email,
          |  mpredict_phone('请联系 13634282910') as phone,
          |  mpredict_qqwechat('扣扣 527153688@qq.com') as qqwechat,
          |  mpredict_url('www.baidu.com www.baidu.com www.baidu.com') as url,
          |  mpredict_pic('img.dxycdn.com豆腐豆腐   img.dxycdn.com  ddfd img.dxycdn.com') as pic,
          |  mpredict_blank('dog,rabbit.我  在  这 我 我 我 里123,22') as blank,
          |  mpredict_chinese('dog,rabbit.我  在  这 我 我 我 里123,22') as chinese,
          |  mpredict_english('dog,rabbit.我  在  这 我 我 我 里123,22') as english,
          |  mpredict_number('dog,rabbit.我  在  这 我 我 我 里123,22') as number,
          |  mpredict_punctuation('dog,rabbit.我  在  这 我 我 我 里123,22') as punctuation,
          |  mpredict_mostchar('dog,rabbit.我  在  这 我 我 我 里123,22') as mostchar,
          |  mpredict_length('dog,rabbit.我  在  这 我 我 我 里123,22') as length
          |  """.stripMargin)
        .collect().mkString(",") == "[true,true,true,0,3,25,21,28,15,9,8,32]")


    }
  }


  "SQLTfIdfInPlace" should "work fine" in {
    withContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      analyzerContext(() => {
        //执行sql
        implicit val spark = runtime.sparkSession
        val dataRDD = spark.sparkContext.parallelize(Seq("我是天才，你呢", "你真的很棒", "天才你好")).map { f =>
          Row.fromSeq(Seq(f))
        }
        val df = spark.createDataFrame(dataRDD,
          StructType(Seq(StructField("content", StringType))))

        df.write.mode(SaveMode.Overwrite).parquet("/tmp/william/tmp/tfidf/df")

        writeStringToFile("/tmp/tfidf/stopwords", List("你").mkString("\n"))
        writeStringToFile("/tmp/tfidf/dics", List("天才").mkString("\n"))
        writeStringToFile("/tmp/tfidf/prioritywords", List("天才").mkString("\n"))

        var newSession = spark
        executeCodeWithCallback(runtime,
          """
            |load parquet.`/tmp/tfidf/df`
            |as orginal_text_corpus;
            |
            |-- 把文本字段转化为tf/idf向量,可以自定义词典
            |train orginal_text_corpus as TfIdfInPlace.`/tmp/tfidfinplace`
            |where inputCol="content"
            |-- 分词相关配置
            |and ignoreNature="true"
            |-- 停用词路径
            |and stopWordPath="/tmp/tfidf/stopwords"
            |-- 高权重词路径
            |and priorityDicPath="/tmp/tfidf/prioritywords"
            |and dicPaths="/tmp/tfidf/dics"
            |-- 高权重词加权倍数
            |and priority="5.0"
            |and nGrams="2"
            |-- and split=""
            |as result;
            |
            |register TfIdfInPlace.`/tmp/tfidfinplace` as jack;
            |select * from result as output;
          """.stripMargin, (df: DataFrame) => {
            df.write.mode(SaveMode.Overwrite).parquet("/tmp/william/tmp/tfidfinplace/data")
            newSession = df.sparkSession
          })

        SharedObjManager.clear
        // we should make sure train vector and predict vector the same
        val trainVector = newSession.sql("select * from parquet.`/tmp/william/tmp/tfidfinplace/data`").toJSON.collect()
        trainVector.foreach {
          f => println(f)
        }
        val predictVector = newSession.sql("select jack(content) as content from orginal_text_corpus").toJSON.collect()
        predictVector.foreach { f =>
          assume(trainVector.contains(f))
        }
      })

    }
  }

  //  "SQLWord2VecInPlace" should "work fine" in {
  //    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
  //      //执行sql
  //
  //      analyzerContext(() => {
  //        val resultFeatures = Seq("index", "merge", "flat", "")
  //        for (resultFeature <- resultFeatures) {
  //          implicit val spark = runtime.sparkSession
  //          delDir("/tmp/william/tmp/word2vecinplace")
  //          delDir("/tmp/william/tmp/tfidf")
  //          val raw = Seq("我是天才，你呢", "你真的很棒", "天才你好")
  //          val dataRDD = spark.sparkContext.parallelize(raw).map { f =>
  //            Row.fromSeq(Seq(f))
  //          }
  //          val df = spark.createDataFrame(dataRDD,
  //            StructType(Seq(StructField("content", StringType))))
  //
  //          df.write.mode(SaveMode.Overwrite).parquet("/tmp/william/tmp/tfidf/df")
  //
  //          writeStringToFile("/tmp/tfidf/stopwords", List("你").mkString("\n"))
  //          writeStringToFile("/tmp/tfidf/prioritywords", List("天才").mkString("\n"))
  //          val sq = createSSEL
  //          ScriptSQLExec.parse(TemplateMerge.merge(
  //            """
  //              |load parquet.`/tmp/tfidf/df`
  //              |as orginal_text_corpus;
  //              |
  //              |-- 把文本字段转化为tf/idf向量,可以自定义词典
  //              |train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vecinplace`
  //              |where inputCol="content"
  //              |-- 分词相关配置
  //              |and ignoreNature="true"
  //              |-- 停用词路径
  //              |and stopWordPath="/tmp/tfidf/stopwords"
  //              |and resultFeature="${resultFeature}"
  //              |and minCount="${minCount}"
  //              |;
  //              |
  //              |register Word2VecInPlace.`/tmp/word2vecinplace` as jack;
  //            """.stripMargin,
  //            Map("resultFeature" -> resultFeature, "minCount" -> "1")), sq)
  //          // we should make sure train vector and predict vector the same
  //          val trainVector = spark.sql("select * from parquet.`/tmp/william/tmp/word2vecinplace/data`").toJSON.collect()
  //          val predictVector = spark.sql("select jack(content) as content from orginal_text_corpus").toJSON.collect()
  //
  //          println("resultFeature:" + resultFeature)
  //          predictVector.foreach { f =>
  //            assume(trainVector.contains(f))
  //          }
  //        }
  //      })
  //    }
  //  }


  "SQLWord2VecInPlaceSplit" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      analyzerContext(() => {
        val splits = Seq("", "，")
        val resultFeatures = Seq("index", "")
        for (split <- splits) {
          for (resultFeature <- resultFeatures) {
            implicit val spark = runtime.sparkSession
            delDir("/tmp/william/tmp/word2vecinplace")
            delDir("/tmp/william/tmp/tfidf")
            val raw = Seq("我是天才，你呢", "你真的很棒", "天才你好")
            val dataRDD = spark.sparkContext.parallelize(raw).map { f =>
              Row.fromSeq(Seq(f))
            }
            val df = spark.createDataFrame(dataRDD,
              StructType(Seq(StructField("content", StringType))))

            df.write.mode(SaveMode.Overwrite).parquet("/tmp/william/tmp/tfidf/df")

            val sq = createSSEL
            ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("word2vecplaceSplit"),
              Map("wordvecPaths" -> "", "resultFeature" -> resultFeature, "split" -> split)), sq)
            // we should make sure train vector and predict vector the same
            val trainVector = spark.sql("select * from parquet.`/tmp/william/tmp/word2vecinplace/data`").toJSON.collect()
            val predictVector = spark.sql("select jack(content) as content from orginal_text_corpus").toJSON.collect()
            val sizeVector = spark.sql("select size(content) as size from parquet.`/tmp/william/tmp/word2vecinplace/data`").toJSON.collect()

            val sizeArray = raw.map(f => {
              "{\"size\":" + f.split(split).size + "}"
            })
            predictVector.foreach { f =>
              assume(trainVector.contains(f))
            }
            sizeVector.foreach { f =>
              assume(sizeArray.contains(f))
            }
          }
        }
      })

    }
  }


  "SQLConfusionMatrix" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq("cat", "dog"),
        Seq("cat", "cat"),
        Seq("rabbit", "cat"),
        Seq("rabbit", "rabbit"),
        Seq("dog", "cat"),
        Seq("cat", "cat"),
        Seq("dog", "dog"))).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("actual", StringType),
          StructField("predict", StringType)
        )))
      df.createOrReplaceTempView("confusionMatrixData")
      df.printSchema()
      df.show()

      val sq = createSSEL
      loadSQLScriptStr("confusion-matrix")
      ScriptSQLExec.parse(loadSQLScriptStr("confusion-matrix"), sq)
      spark.sql("select * from parquet.`/tmp/william/tmp/confusionMatrix/data`").show(100, false)
      spark.sql("select * from parquet.`/tmp/william/tmp/confusionMatrix/detail`").show(100, false)
    }
  }


  "SQLScalerInPlace" should "work fine" in {
    withContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq(1.0, 2.0, 3.0),
        Seq(1.0, 4.0, 3.0),
        Seq(1.0, 7.0, 3.0))).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("a", DoubleType),
          StructField("b", DoubleType),
          StructField("c", DoubleType)
        )))
      df.createOrReplaceTempView("orginal_text_corpus")

      var newSession = spark
      executeCodeWithCallback(runtime,
        """
          |-- 把文本字段转化为tf/idf向量,可以自定义词典
          |train orginal_text_corpus as ScalerInPlace.`/tmp/scaler`
          |where inputCols="a,b"
          |-- 使用是什么缩放方法
          |and scaleMethod="min-max"
          |-- 是否自动修正异常值
          |and removeOutlierValue="false"
          |;
          |
          |register ScalerInPlace.`/tmp/scaler` as jack;
        """.stripMargin, (df: DataFrame) => {
          newSession = df.sparkSession
        })

      // we should make sure train vector and predict vector the same
      val trainVector = newSession.sql("select * from parquet.`/tmp/william/tmp/scaler/data`").toJSON.collect()
      val predictVector = newSession.sql("select jack(array(a,b))[0] a,jack(array(a,b))[1] b, c from orginal_text_corpus").toJSON.collect()
      predictVector.foreach(println(_))
      trainVector.foreach(println(_))
      predictVector.foreach { f =>
        assume(trainVector.contains(f))
      }

    }
  }

  "SQLNormalizeInPlace" should "work fine" in {
    withContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq(1.0, 2.0, 3.0),
        Seq(1.0, 4.0, 3.0),
        Seq(1.0, 7.0, 3.0))).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("a", DoubleType),
          StructField("b", DoubleType),
          StructField("c", DoubleType)
        )))
      df.createOrReplaceTempView("orginal_text_corpus")


      def validate(spark:SparkSession) = {
        val trainVector = spark.sql("select * from parquet.`/tmp/william/tmp/scaler2/data`").toJSON.collect()
        val predictVector = spark.sql("select jack(array(a,b))[0] a,jack(array(a,b))[1] b, c from orginal_text_corpus").toJSON.collect()
        predictVector.foreach(println(_))
        trainVector.foreach(println(_))
        predictVector.foreach { f =>
          assume(trainVector.contains(f))
        }
      }

      val code =
        """
          |-- 把文本字段转化为tf/idf向量,可以自定义词典
          |train orginal_text_corpus as NormalizeInPlace.`/tmp/scaler2`
          |where inputCols="a,b"
          |-- 使用是什么缩放方法
          |and method="${method}"
          |-- 是否自动修正异常值
          |and removeOutlierValue="false"
          |;
          |
          |register NormalizeInPlace.`/tmp/scaler2` as jack;
        """.stripMargin
      var session = spark
      executeCodeWithCallback(runtime, TemplateMerge.merge(code, Map("method" -> "standard"))
        , (df: DataFrame) => {
          session = df.sparkSession
        })

      // we should make sure train vector and predict vector the same
      validate(session)

      executeCodeWithCallback(runtime, TemplateMerge.merge(code, Map("method" -> "p-norm"))
        , (df: DataFrame) => {
          session = df.sparkSession
        })
      validate(session)
    }
  }

  "SQLVecMapInPlace" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq(Map("wow" -> 1, "wow2" -> 7)),
        Seq(Map("wow2" -> 1)),
        Seq(Map("wow3" -> 5)))).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("a", MapType(StringType, IntegerType))
        )))

      val path = "/tmp/wow"
      val params = Map("inputCol" -> "a")
      val vecMap = new SQLVecMapInPlace()
      vecMap.train(df, path, params)
      spark.sql("select * from parquet.`/tmp/wow/data`").show()
      val model = vecMap.load(spark, path, params)
      val jack = vecMap.predict(spark, model, "jack", params)
      spark.udf.register("vecToMap", jack)
      val res = spark.sql(
        s"""
            select vecToMap(map_value_int_to_double(map("wow",9)))
         """.stripMargin).collect().head

      val vec = res.getAs[Vector](0)
      assume(vec.toDense.toArray.mkString(",") == "0.0,0.0,9.0")

    }
  }

  "SQLCorpusExplainInPlace" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq(1),
        Seq(1),
        Seq(0), Seq(1)
      )).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("label", IntegerType)
        )))

      val path = "/tmp/wow"
      val params = Map("labelCol" -> "label")
      val corpusExplain = new SQLCorpusExplainInPlace()
      corpusExplain.train(df, path, params)
      spark.sql("select * from parquet.`/tmp/wow/data`").collect().foreach(f => assume(f.size == 5))

    }
  }

  "SQLStringIndex" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq("a"),
        Seq("b"),
        Seq("c")
      )).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("st", StringType)
        )))
      df.createOrReplaceTempView("stringIndex")
      val sq = createSSEL

      ScriptSQLExec.parse("train stringIndex as StringIndex.`/tmp/model` where inputCol=\"st\";register StringIndex.`/tmp/model` as mpredict;", sq)
      val res = spark.sql("select mpredict_r(mpredict(st)) as st from stringIndex").toJSON.collect()
      val ori = spark.sql("select st from stringIndex").toJSON.collect()
      res.foreach(f =>
        assume(ori.contains(f))
      )
    }
  }

  "SQLStringIndexArray" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq(Seq("a1", "b1", "c1")),
        Seq(Seq("a2", "b2")),
        Seq(Seq("a3", "b3", "d3"))
      )).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("st", ArrayType(StringType))
        )))
      df.createOrReplaceTempView("stringIndex")
      val sq = createSSEL

      ScriptSQLExec.parse("train stringIndex as StringIndex.`/tmp/model` where inputCol=\"st\";register StringIndex.`/tmp/model` as mpredict;", sq)
      val res = spark.sql("select mpredict_rarray(mpredict_array(st)) as st from stringIndex").toJSON.collect()
      val ori = spark.sql("select st from stringIndex").toJSON.collect()
      res.foreach(f =>
        assume(ori.contains(f))
      )
    }
  }

  "SQLWord2ArrayInPlace" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      analyzerContext(() => {
        //执行sql
        implicit val spark = runtime.sparkSession
        val raw = Seq("我是天才，你呢", "你真的很棒", "天才你好")
        val dataRDD = spark.sparkContext.parallelize(raw).map { f =>
          Row.fromSeq(Seq(f))
        }
        val df = spark.createDataFrame(dataRDD,
          StructType(Seq(StructField("content", StringType))))
        df.createOrReplaceTempView("t1")
        val sq = createSSEL

        ScriptSQLExec.parse("train t1 as Word2VecInPlace.`/tmp/word2vec` where inputCol=\"content\" and split=\"\";" +
          "train t1 as Word2ArrayInPlace.`/tmp/word2array` where modelPath=\"/tmp/william/tmp/word2vec\";" +
          "register Word2ArrayInPlace.`/tmp/word2array` as jack;", sq)
        val res1 = spark.sql("select jack(\"你我，他\") as st").toJSON.collect()
        res1.foreach(f =>
          assume(f.equals("{\"st\":[\"你\",\"我\",\"，\"]}"))
        )

        ScriptSQLExec.parse("train t1 as TfIdfInPlace.`/tmp/tfidf` where inputCol=\"content\" and split=\"\";" +
          "train t1 as Word2ArrayInPlace.`/tmp/word2array` where modelPath=\"/tmp/william/tmp/tfidf\";" +
          "register Word2ArrayInPlace.`/tmp/word2array` as jack;", sq)
        val res2 = spark.sql("select jack(\"你我，他\") as st").toJSON.collect()
        res2.foreach(f =>
          assume(f.equals("{\"st\":[\"你\",\"我\",\"，\"]}"))
        )
      })

    }
  }
  "SQLCommunityBasedSimilarityInPlace" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val rdd = spark.sparkContext.parallelize(Seq((3L, 7L, 0.9), (5L, 3L, 0.9),
        (2L, 5L, 0.1), (5L, 7L, 0.8),
        (4L, 0L, 0.9), (5L, 0L, 0.2))).map { f =>
        Row(f._1, f._2, f._3)
      }
      val relationships = spark.createDataFrame(rdd,
        StructType(Seq(StructField("i", LongType), StructField("j", LongType), StructField("v", DoubleType))))

      val ssip = new SQLCommunityBasedSimilarityInPlace()
      ssip.train(relationships, "/tmp/jack", Map(
        "minCommunitySize" -> "2"
      ))
      spark.sql("select * from parquet.`/tmp/jack/data`").show(false)
      val res = spark.sql("select * from parquet.`/tmp/jack/data`").head.getSeq(1)
      assume(res.mkString(",") == "3,7,5")
    }
  }
  "SQLRawSimilarInPlace" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      analyzerContext(() => {
        implicit val spark = runtime.sparkSession
        val dataRDD = spark.sparkContext.parallelize(Seq(
          Seq("我是天才。你呢，早上吃饭没", 1L),
          Seq("你真的很棒，晚上吃饭没", 2L),
          Seq("我是天才。你呢，早上吃饭没", 0L))).map { f =>
          Row.fromSeq(f)
        }

        val df = spark.createDataFrame(dataRDD,
          StructType(Seq(
            StructField("content", StringType),
            StructField("label", LongType)
          )))
        df.createOrReplaceTempView("t1")
        val sq = createSSEL
        //训练得到word2vec模型
        ScriptSQLExec.parse("train t1 as Word2VecInPlace.`/tmp/word2vec` where inputCol=\"content\";", sq)

        ScriptSQLExec.parse("train t1 as RawSimilarInPlace.`/tmp/rawsimilar` where modelPath=\"/tmp/william/tmp/word2vec\";" +
          "register RawSimilarInPlace.`/tmp/rawsimilar` as jack;", sq)
        val res = spark.sql("select label,jack(label,0.9) as result from t1").toJSON.collect()
        assume(JSONObject.fromObject(res(0)).get("result").toString.equals("{\"0\":1}"))
        assume(JSONObject.fromObject(res(1)).get("result").toString.equals("{}"))
        assume(JSONObject.fromObject(res(2)).get("result").toString.equals("{\"1\":1}"))
      })
    }
  }
}

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

package streaming.test.mmlib

import java.io.File

import org.apache.spark.SparkCoreVersion
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.pojo.Rating
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs._

/**
  * Created by allwefantasy on 13/9/2018.
  */
class MLLibSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  copySampleMovielensRratingsData
  copySampleLibsvmData
  copyTitanic

  "als" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      import spark.implicits._
      ScriptSQLExec.contextGetOrForTest()

      val ratings = spark.read.textFile("/tmp/william/sample_movielens_ratings.txt")
        .map { str =>
          val fields = str.split("::")
          Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
        }
        .toDF()
      val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
      test.createOrReplaceTempView("test")
      val als = new SQLALSInPlace()
      val modelPath = "/tmp/als"
      als.train(training, "/tmp/william" + modelPath, Map(
        "fitParam.0.maxIter" -> "5",
        "fitParam.0.regParam" -> "0.01",
        "fitParam.0.userCol" -> "userId",
        "fitParam.0.itemCol" -> "movieId",
        "fitParam.0.ratingCol" -> "rating",
        "fitParam.1.maxIter" -> "1",
        "fitParam.1.regParam" -> "0.1",
        "fitParam.1.userCol" -> "userId",
        "fitParam.1.itemCol" -> "movieId",
        "fitParam.1.ratingCol" -> "rating",
        "evaluateTable" -> "test",
        "userRec" -> "10"
      ))
      val finalModelPath = SQLPythonFunc.getAlgMetalPath("/tmp/william/tmp/als", true) + "/0"
      spark.sql(s"select * from parquet.`$finalModelPath`").show()


      als.train(training, "/tmp/william" + modelPath, Map(
        "fitParam.0.maxIter" -> "1",
        "fitParam.0.regParam" -> "0.0001",
        "fitParam.0.userCol" -> "userId",
        "fitParam.0.itemCol" -> "movieId",
        "fitParam.0.ratingCol" -> "rating",
        "fitParam.0.userRec" -> "10",
        "fitParam.0.evaluateTable" -> "test"
      ))

      assume(new File("/tmp/william//tmp/als/_model_1").exists())
    }
  }

  "unbalance_sample" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      ScriptSQLExec.contextGetOrForTest()
      val sq = createSSEL

      ScriptSQLExec.parse(
        """
          |load libsvm.`/sample_libsvm_data.txt` as data;
          |
          |train data as NaiveBayes.`/tmp/bayes_model` where multiModels="true";
          |
          |register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
          |
          |select bayes_predict(features) as predict_label, label  from data as result;
          |
          |save overwrite result as json.`/tmp/result`;
          |
          |select * from result as output;
        """.stripMargin, sq)
      val res = spark.sql("select * from output").show(false)

    }
  }

  "SQLRandomForest" should "work fine" in {
    copySampleLibsvmData
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val randomForest = new SQLRandomForest()
      ScriptSQLExec.contextGetOrForTest()

      val df = spark.read.format("libsvm").load("/tmp/william/sample_libsvm_data.txt")
      df.createOrReplaceTempView("data")
      randomForest.train(df, "/tmp/SQLRandomForest", Map(
        "keepVersion" -> "true",
        "evaluateTable" -> "data",
        "fitParam.0.maxDepth" -> "3"
      ))
      val models = randomForest.load(spark, "/tmp/SQLRandomForest", Map("autoSelectByMetric" -> "f1"))
      val udf = randomForest.predict(spark, models, "jack", Map("autoSelectByMetric" -> "f1"))
      spark.udf.register("jack", udf)
      df.selectExpr("jack(features) as predict").show()
    }
  }

  "SQLLDA" should "work fine" in {
    copySampleLibsvmData
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      val coreCompatibility = new SQLLDA().coreCompatibility.filter(f => f.coreVersion == SparkCoreVersion.version).size > 0
      if (coreCompatibility) {
        implicit val spark = runtime.sparkSession
        val sqlLDA = new SQLLDA()
        ScriptSQLExec.contextGetOrForTest()

        val df = spark.read.format("libsvm").load("/tmp/william/sample_lda_libsvm_data.txt")
        df.createOrReplaceTempView("data")
        sqlLDA.train(df, "/tmp/SQLLDA", Map(
          "k" -> "3",
          "topicConcentration" -> "3.0",
          "docConcentration" -> "3.0",
          "optimizer" -> "online",
          "checkpointInterval" -> "10",
          "maxIter" -> "100"
        ))
        val models = sqlLDA.load(spark, "/tmp/SQLLDA", Map())
        val udf = sqlLDA.predict(spark, models, "jack", Map())
        spark.udf.register("jack", udf)
        spark.sql("select label,jack(4) topicsMatrix,jack_doc(features) TopicDistribution,jack_topic(label,4) describeTopics " +
          "from data as result").show()
      }
    }
  }

  "KMeans" should "work fine" in {
    copySampleLibsvmData
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val randomForest = new SQLKMeans()
      ScriptSQLExec.contextGetOrForTest()

      val df = spark.read.format("libsvm").load("/tmp/william/sample_libsvm_data.txt")
      df.createOrReplaceTempView("data")
      randomForest.train(df, "/tmp/KMeans", Map(
        "keepVersion" -> "true",
        "evaluateTable" -> "data",
        "fitParam.0.k" -> "2"
      ))
      var models = randomForest.load(spark, "/tmp/KMeans", Map("autoSelectByMetric" -> "silhouette"))
      var udf = randomForest.predict(spark, models, "jack", Map("autoSelectByMetric" -> "silhouette"))
      spark.udf.register("jack", udf)
      df.selectExpr("jack(features) as predict").show()


      models = randomForest.load(spark, "/tmp/KMeans", Map())
      udf = randomForest.predict(spark, models, "jack", Map())
      spark.udf.register("jack", udf)
      df.selectExpr("jack(features) as predict").show()


      randomForest.train(df, "/tmp/KMeans", Map(
        "keepVersion" -> "true",
        "fitParam.0.k" -> "2"))

      models = randomForest.load(spark, "/tmp/KMeans", Map())
      udf = randomForest.predict(spark, models, "jack", Map())
      spark.udf.register("jack", udf)
      df.selectExpr("jack(features) as predict").show()
    }
  }

  "GBTs" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val randomForest = new SQLGBTs()
      ScriptSQLExec.contextGetOrForTest()

      val df = spark.read.format("libsvm").load("/tmp/william/sample_libsvm_data.txt")
      df.createOrReplaceTempView("data")
      randomForest.train(df, "/tmp/GBTs", Map(
        "keepVersion" -> "true",
        "evaluateTable" -> "data",
        "fitParam.0.maxDepth" -> "2"
      ))
      val models = randomForest.load(spark, "/tmp/GBTs", Map("autoSelectByMetric" -> "f1"))
      val udf = randomForest.predict(spark, models, "jack", Map("autoSelectByMetric" -> "f1"))
      spark.udf.register("jack", udf)
      df.selectExpr("jack(features) as predict").show()
    }
  }


}

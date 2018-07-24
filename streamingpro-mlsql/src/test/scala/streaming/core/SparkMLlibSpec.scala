package streaming.core

import java.io.File

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.streaming.BasicSparkOperation
import streaming.common.ShellCommand
import streaming.core.pojo.Rating
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.{SQLALSInPlace, SQLPythonFunc}
import streaming.dsl.template.TemplateMerge


/**
  * Created by allwefantasy on 2/5/2017.
  */
class SparkMLlibSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  copySampleMovielensRratingsData
  "als" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      import spark.implicits._


      ShellCommand.exec("rm -rf /tmp/william//tmp/als")

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

}


package streaming.core

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.pojo.Rating
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.SQLALSInPlace
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
        "maxIter" -> "5",
        "regParam" -> "0.01",
        "userCol" -> "userId",
        "itemCol" -> "movieId",
        "ratingCol" -> "rating",
        "userRec" -> "10"
      ))

      val sq = createSSEL

      ScriptSQLExec.parse(
        s"""
           |register ALSInPlace.`${modelPath}` as rmse options evaluateTable="test";
         """.stripMargin, sq)

      spark.sql("select rmse()").show()
    }
  }

}


package streaming

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.{SQLKMeans, SQLRandomForest}

/**
  * Created by allwefantasy on 13/9/2018.
  */
class MLLibSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
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
      val models = randomForest.load(spark, "/tmp/KMeans", Map("autoSelectByMetric" -> "silhouette"))
      val udf = randomForest.predict(spark, models, "jack", Map("autoSelectByMetric" -> "silhouette"))
      spark.udf.register("jack", udf)
      df.selectExpr("jack(features) as predict").show()
    }
  }
}

package streaming.core

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.mmlib.algs.StringFeature

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
      val newDF = StringFeature.tfidf(df, "/tmp/mapping", "", "content")
      newDF.show()

    }
  }
}

package streaming.core

import java.io.File
import java.sql.Timestamp

import net.sf.json.JSONObject
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.ss.NoopForeachWriter
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 26/4/2018.
  */
class SSSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  "ss-append-test" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //      val sq = createSSEL
      //      ScriptSQLExec.parse(loadSQLScriptStr("load-non-mlsql-sklearn-model"), sq)

      import spark.implicits._
      val inputStream = new MemoryStream[(Timestamp, String)](1, spark.sqlContext)
      inputStream.addData((new Timestamp(System.currentTimeMillis()), "a"),
        (new Timestamp(System.currentTimeMillis()), "b"))

      val aggregatedStream = inputStream.toDS().toDF("created", "name")
        .withWatermark("created", "10 seconds")

      aggregatedStream.createOrReplaceTempView("jack")
      var newDF = spark.sql("select created,count(*) as cc from jack  group by created ")
      //newDF = newDF.withWatermark("created", "30 seconds")
      newDF.createOrReplaceTempView("jack1")
      newDF = spark.sql("select sum(cc) as from jack1  group by created ")

      val query = newDF.writeStream.outputMode("append")
        .foreach(new NoopForeachWriter[Row]()).start()
      query.awaitTermination(3000)
    }

  }
}






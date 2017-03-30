package org.apache.spark.sql.execution.streaming

import net.csdn.modules.http.RestResponse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener}

/**
  * Created by allwefantasy on 28/3/2017.
  */
class SQLExecute(spark: SparkSession, restResponse: RestResponse) {
  def query(sql: String) = {
    val ds = spark.sql(sql)
    val sink = new ForeachHttpSink(restResponse)
    val df = ds.toDF()
    val streamQuery = spark.sessionState.streamingQueryManager.startQuery(
      None,
      None,
      df,
      sink,
      OutputMode.Append(),useTempCheckpointLocation = true
    )
    streamQuery.awaitTermination(5000)
  }
}

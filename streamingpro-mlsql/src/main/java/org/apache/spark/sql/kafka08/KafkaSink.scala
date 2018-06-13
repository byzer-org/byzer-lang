package org.apache.spark.sql.kafka08

import java.{util => ju}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.streaming.Sink

/**
  * Created by allwefantasy on 4/7/2017.
  */
class KafkaSink(
                                  sqlContext: SQLContext,
                                  executorKafkaParams: ju.Map[String, Object],
                                  topic: Option[String]) extends Sink with Logging {
  @volatile private var latestBatchId = -1L

  override def toString(): String = "Kafka8Sink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      KafkaWriter.write(sqlContext.sparkSession,
        data.queryExecution, executorKafkaParams, topic)
      latestBatchId = batchId
    }
  }
}

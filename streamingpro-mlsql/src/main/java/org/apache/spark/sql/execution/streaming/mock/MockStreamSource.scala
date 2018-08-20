package org.apache.spark.sql.execution.streaming.mock

import java.nio.charset.Charset
import java.util.UUID

import org.apache.kafka.common.TopicPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


/**
  * Created by allwefantasy on 20/8/2018.
  */
class MockStreamSource(
                        sqlContext: SQLContext,
                        sourceOptions: Map[String, String],
                        metadataPath: String
                      ) extends Source with Logging {

  private val sc = sqlContext.sparkContext

  override def schema: StructType = MockStreamSource.schema

  override def getOffset: Option[Offset] = {
    Some(new MockSourceOffset(Map(
      new TopicPartition("test", 0) -> 0l
    )))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val table = sourceOptions("path")
    val df = sqlContext.sparkSession.table(table)
    val dSchema = df.schema
    val rdd = df.rdd.map { f =>
      val timestamp = DateTime.parse(f.getString(dSchema.fieldIndex("timestamp")), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"))
      InternalRow(
        f.getString(dSchema.fieldIndex("key")).getBytes(Charset.forName("utf-8")),
        f.getString(dSchema.fieldIndex("value")).getBytes(Charset.forName("utf-8")),
        UTF8String.fromString(f.getString(dSchema.fieldIndex("topic"))),
        f.getLong(dSchema.fieldIndex("partition")).toInt,
        f.getLong(dSchema.fieldIndex("offset")),
        DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(timestamp.getMillis)),
        f.getLong(dSchema.fieldIndex("timestampType")).toInt)
    }
    sqlContext.internalCreateDataFrame(rdd, schema)
  }

  override def stop(): Unit = {

  }
}

object MockStreamSource {
  val schema = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType)
  ))
}

class MockStreamSourceProvider extends DataSourceRegister with StreamSourceProvider with Logging {
  override def shortName(): String = "mockStream"

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = {
    (shortName(), MockStreamSource.schema)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    val uniqueGroupId = s"spark-mockStream-source-${UUID.randomUUID}-${metadataPath.hashCode}"
    new MockStreamSource(sqlContext, parameters, metadataPath)
  }
}

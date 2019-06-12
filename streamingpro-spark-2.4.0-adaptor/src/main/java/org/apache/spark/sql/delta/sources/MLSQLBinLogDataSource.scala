package org.apache.spark.sql.delta.sources

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.Event
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * 2019-06-11 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLBinLogDataSource extends StreamSourceProvider with DataSourceRegister {
  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = ???

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    val client = new BinaryLogClient("hostname", 3306, "username", "password")
    val eventDeserializer = new EventDeserializer()
    eventDeserializer.setCompatibilityMode(
      EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
      EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
    )
    client.setEventDeserializer(eventDeserializer)

    client.registerEventListener(new BinaryLogClient.EventListener() {
      def onEvent(event: Event): Unit = {
        val eventType = event.getHeader
        eventType match {
          case TABLE_MAP =>
            val tableMapEventData = event.getData
          // handle tableMapEventData (contains mapping between *RowsEventData::tableId and table name
          // that can be used (together with https://github.com/shyiko/mysql-binlog-connector-java/issues/24#issuecomment-43747417)
          // to map column names to the values)

          case PRE_GA_WRITE_ROWS =>
          case WRITE_ROWS =>
          case EXT_WRITE_ROWS =>
            val writeRowsEventData = event.getData
          // handle writeRowsEventData (generated when someone INSERTs data)

          case PRE_GA_UPDATE_ROWS =>
          case UPDATE_ROWS =>
          case EXT_UPDATE_ROWS =>
            val updateRowsEventData = event.getData
          // handle updateRowsEventData (generated when someone UPDATEs data)
          // THIS IS THE EVENT your are looking for

          case PRE_GA_DELETE_ROWS =>
          case DELETE_ROWS =>
          case EXT_DELETE_ROWS =>
            val deleteRowsEventData = event.getData
          // handle deleteRowsEventData (generated when someone DELETEs data)

          case _ =>
        }
      }
    })
    client.connect()
    MLSQLBinLogSource(client,sqlContext.sparkSession,Map())
  }

  override def shortName(): String = "mysql-binglog"
}

case class MLSQLBinLogSource( client:BinaryLogClient,
                              spark: SparkSession, parameters: Map[String, String]
                            ) extends Source {
  override def schema: StructType = ???

  override def getOffset: Option[Offset] = ???

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = ???

  override def stop(): Unit = ???
}

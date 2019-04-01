package streaming.core.datasource.impl

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource.{DataSinkConfig, DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLKafka(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {

    def getSubscribe = {
      if (shortFormat == "kafka8" || shortFormat == "kafka9") {
        "topics"
      } else "subscribe"
    }

    // ignore the reader since this reader is not stream reader
    val streamReader = config.df.get.sparkSession.readStream
    val format = config.config.getOrElse("implClass", fullFormat)
    if (!config.path.isEmpty) {
      streamReader.option(getSubscribe, config.path)
    }
    streamReader.options(rewriteConfig(config.config)).format(format).load()
  }

  def isStream = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.env().contains("streamName")
  }


  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    def getUrl = {
      if (shortFormat == "kafka8" || shortFormat == "kafka9") {
        "metadata.broker.list"
      } else "kafka.bootstrap.servers"
    }

    def getKafkaBrokers = {
      getUrl -> config.config.getOrElse("metadata.broker.list", config.config.get("kafka.bootstrap.servers").get)
    }

    def getWriteTopic = {
      if (shortFormat == "kafka8" || shortFormat == "kafka9") {
        "topics"
      } else "topic"
    }

    if (getKafkaBrokers._2 == null || getKafkaBrokers._2.isEmpty) {
      throw new MLSQLException("metadata.broker.list or kafka.bootstrap.servers are required")
    }
    if (isStream) {
      return super.save(batchWriter, config.copy(config = ((config.config - "metadata.broker.list" - "kafka.bootstrap.servers") ++ Map(
        getWriteTopic -> config.path,
        getKafkaBrokers._1 -> getKafkaBrokers._2
      ))))

    }

    batchWriter.options((config.config - "metadata.broker.list" - "kafka.bootstrap.servers")).option(getWriteTopic, config.path).
      option(getKafkaBrokers._1, getKafkaBrokers._2).format(fullFormat).save()

  }

  override def fullFormat: String = "kafka"

  override def shortFormat: String = "kafka"

  final val kafkaBootstrapServers: Param[String] = new Param[String](this, "kafka.bootstrap.servers", "host1:port1,host2:port2")
  final val startingOffsets: Param[String] = new Param[String](this, "startingOffsets", "only for 0.10.0 or higher;{\"topic1\":{\"0\":23,\"1\":-2},\"topic2\":{\"0\":-2}}")
  final val endingOffsets: Param[String] = new Param[String](this, "startingOffsets", "only for 0.10.0 or higher;{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}")

}

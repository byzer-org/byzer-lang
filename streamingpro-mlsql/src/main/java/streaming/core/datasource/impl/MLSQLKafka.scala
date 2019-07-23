package streaming.core.datasource.impl

import org.apache.spark.ml.param.Param
import org.apache.spark.sql._
import _root_.streaming.core.datasource.{DataSinkConfig, DataSourceConfig, MLSQLBaseStreamSource}
import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLKafka(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {

    if (isStream) {
      // ignore the reader since this reader is not stream reader
      val streamReader = config.df.get.sparkSession.readStream
      val format = config.config.getOrElse("implClass", fullFormat)
      streamReader.options(rewriteKafkaConfig(config.config, getSubscribe, getLoadUrl, config.path)).format(format).
        load()

    } else {
      val format = config.config.getOrElse("implClass", fullFormat)
      reader.options(rewriteKafkaConfig(config.config, getSubscribe, getLoadUrl, config.path)).format(format).
        load()
    }
  }

  def getSubscribe = {
    if (shortFormat == "kafka8" || shortFormat == "kafka9") {
      "topics"
    } else "subscribe"
  }

  def isStream = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.env().contains("streamName")
  }


  def getLoadUrl = {
    "kafka.bootstrap.servers"
  }

  def getSaveUrl = {
    if (shortFormat == "kafka8" || shortFormat == "kafka9") {
      "metadata.broker.list"
    } else "kafka.bootstrap.servers"
  }

  def getKafkaBrokers(config: Map[String, String], url: String) = {
    url -> config.getOrElse("metadata.broker.list", config.get("kafka.bootstrap.servers").get)
  }

  def getWriteTopic = {
    if (shortFormat == "kafka8" || shortFormat == "kafka9") {
      "topics"
    } else "topic"
  }


  def rewriteKafkaConfig(config: Map[String, String], topicKey: String, url: String, path: String): Map[String, String] = {
    var temp = ((config - "metadata.broker.list" - "kafka.bootstrap.servers") ++ Map(
      getKafkaBrokers(config, url)
    ))
    if (path != null && !path.isEmpty) {
      temp = temp ++ Map(topicKey -> path)
    }
    temp
  }

  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    if (isStream) {
      return super.save(batchWriter, config.copy(config = rewriteKafkaConfig(config.config, getWriteTopic, getSaveUrl, config.path)))

    }
    batchWriter.options(rewriteKafkaConfig(config.config, getWriteTopic, getSaveUrl, config.path)).format(fullFormat).save()

  }

  override def fullFormat: String = "kafka"

  override def shortFormat: String = "kafka"

  final val kafkaBootstrapServers: Param[String] = new Param[String](this, "kafka.bootstrap.servers", "host1:port1,host2:port2")
  final val startingOffsets: Param[String] = new Param[String](this, "startingOffsets", "only for 0.10.0 or higher;{\"topic1\":{\"0\":23,\"1\":-2},\"topic2\":{\"0\":-2}}")
  final val endingOffsets: Param[String] = new Param[String](this, "endingOffsets", "only for 0.10.0 or higher;{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}")

}

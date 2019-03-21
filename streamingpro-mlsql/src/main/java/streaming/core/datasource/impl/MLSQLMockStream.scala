package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource.{DataSinkConfig, DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLMockStream(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val streamReader = config.df.get.sparkSession.readStream
    val format = config.config.getOrElse("implClass", fullFormat)
    streamReader.options(rewriteConfig(config.config) ++ Map("path" -> config.path)).format(format).load()
  }


  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    throw new RuntimeException(s"save is not supported in ${shortFormat}")
  }

  override def fullFormat: String = "org.apache.spark.sql.execution.streaming.mock.MockStreamSourceProvider"

  override def shortFormat: String = "mockStream"
}

package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import streaming.core.datasource.{DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLKafka(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    // ignore the reader since this reader is not stream reader
    val streamReader = config.df.get.sparkSession.readStream
    val format = config.config.getOrElse("implClass", fullFormat)
    if (!config.path.isEmpty) {
      streamReader.option("subscribe", config.path)
    }
    streamReader.options(rewriteConfig(config.config)).format(format).load()
  }


  override def fullFormat: String = "kafka"

  override def shortFormat: String = "kafka"
}

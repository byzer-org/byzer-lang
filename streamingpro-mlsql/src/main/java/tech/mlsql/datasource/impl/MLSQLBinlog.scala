package tech.mlsql.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import streaming.core.datasource.{DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-06-14 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLBinlog(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val streamReader = config.df.get.sparkSession.readStream
    streamReader.options(rewriteConfig(config.config)).format(fullFormat).load()
  }

  override def fullFormat: String = "org.apache.spark.sql.delta.sources.MLSQLBinLogDataSource"

  override def shortFormat: String = "binlog"

}
package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import streaming.core.datasource.{DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}


class MLSQLStreamSolr(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    throw new RuntimeException(s"load is not support by ${shortFormat}")
  }

  override def fullFormat: String = "org.apache.spark.sql.execution.streaming.SolrSinkProvider"

  override def shortFormat: String = "streamSolr"

  override def aliasFormat: String = "solr"
}

package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLCrawlerSql(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    reader.option("path", config.path).options(rewriteConfig(config.config)).format("org.apache.spark.sql.execution.datasources.crawlersql").load()
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    throw new RuntimeException(s"save is not supported in ${shortFormat}")
  }

  override def fullFormat: String = "crawlersql"

  override def shortFormat: String = fullFormat

}


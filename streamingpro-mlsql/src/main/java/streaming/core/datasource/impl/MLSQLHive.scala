package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLHive(override val uid: String) extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with WowParams with Logging with WowLog {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val format = config.config.getOrElse("implClass", fullFormat)
    reader.options(config.config).format(format).table(config.path)
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    writer.format(config.config.getOrElse("file_format", "parquet"))
    val options = config.config - "file_format" - "implClass"
    writer.options(options).mode(config.mode).saveAsTable(config.path)
  }


  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(db, table) = config.path.split("\\.") match {
      case Array(db, table) => Array(db, table)
      case Array(table) => Array("default", table)
    }
    SourceInfo(shortFormat, db, table)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "hive"

  override def shortFormat: String = fullFormat
}

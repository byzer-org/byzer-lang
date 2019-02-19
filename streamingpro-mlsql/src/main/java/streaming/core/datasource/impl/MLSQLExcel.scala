package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._

/**
  * 2019-02-19 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLExcel extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry {
  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val format = config.config.getOrElse("implClass", fullFormat)

    reader.options(config.config).format(format).load(config.path)
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val format = config.config.getOrElse("implClass", fullFormat)
    writer.options(config.config).format(format).save(config.path)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = SourceInfo(shortFormat, "", "")

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "com.crealytics.spark.excel"

  override def shortFormat: String = "excel"
}

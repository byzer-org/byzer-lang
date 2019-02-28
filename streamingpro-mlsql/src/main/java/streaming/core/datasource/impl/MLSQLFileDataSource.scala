package streaming.core.datasource.impl

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.mmlib.algs.param.WowParams
import org.apache.spark.sql._

/**
  * 2019-02-19 WilliamZhu(allwefantasy@gmail.com)
  */
abstract class MLSQLFileDataSource extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with WowParams {

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val format = config.config.getOrElse("implClass", fullFormat)
    reader.options(rewriteConfig(config.config)).format(format).load(config.config("_filePath_"))
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val format = config.config.getOrElse("implClass", fullFormat)

    writer.options(rewriteConfig(config.config)).format(format).save(config.config("_filePath_"))
  }

  def rewriteConfig(config: Map[String, String]): Map[String, String]

  override def sourceInfo(config: DataAuthConfig): SourceInfo = SourceInfo(shortFormat, "", "")

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

}

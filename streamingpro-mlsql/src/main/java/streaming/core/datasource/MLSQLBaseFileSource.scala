package streaming.core.datasource

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.dsl.adaptor.DslTool

/**
  * 2019-03-19 WilliamZhu(allwefantasy@gmail.com)
  */
abstract class MLSQLBaseFileSource extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with DslTool {
  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", fullFormat)
    val owner = config.config.get("owner").getOrElse(context.owner)
    reader.options(rewriteConfig(config.config)).format(format).load(resourceRealPath(context.execListener, Option(owner), config.path))
  }

  def rewriteConfig(config: Map[String, String]) = {
    config
  }


  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", fullFormat)
    val partitionByCol = config.config.getOrElse("partitionByCol", "").split(",").filterNot(_.isEmpty)
    if (partitionByCol.length > 0) {
      writer.partitionBy(partitionByCol: _*)
    }
    writer.options(rewriteConfig(config.config)).mode(config.mode).format(format).save(resourceRealPath(context.execListener, Option(context.owner), config.path))
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    SourceInfo(shortFormat, "", resourceRealPath(context.execListener, Option(owner), config.path))
  }

}

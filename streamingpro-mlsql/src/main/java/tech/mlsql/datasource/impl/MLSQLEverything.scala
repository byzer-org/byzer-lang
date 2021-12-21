package tech.mlsql.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource.{DataAuthConfig, DataSinkConfig, DataSourceConfig, DataSourceRegistry, MLSQLDataSourceKey, MLSQLRegistry, MLSQLSink, MLSQLSource, MLSQLSourceConfig, MLSQLSourceInfo, MLSQLSparkDataSourceType, SourceInfo}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.adaptor.DslTool

/**
 * 20/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLEverything (override val uid: String) extends MLSQLSource
  with MLSQLSink
  with MLSQLSourceInfo
  with MLSQLSourceConfig
  with MLSQLRegistry with DslTool with WowParams with Logging {


  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val session = config.df.get.sparkSession
    import session.implicits._
    session.createDataset[String](Seq("Byzer-lang")).toDF("Hello")
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = ???

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    SourceInfo("system", "everything", config.path)
  }


  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "Everything"

  override def shortFormat: String = "Everything"
}

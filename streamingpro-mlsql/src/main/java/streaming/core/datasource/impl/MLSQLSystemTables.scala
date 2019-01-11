package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import streaming.core.datasource._
import streaming.core.{StreamingproJobInfo, StreamingproJobManager}
import streaming.dsl.ScriptSQLExec

/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLSystemTables extends MLSQLSource with MLSQLSourceInfo with MLSQLRegistry {

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val owner = ScriptSQLExec.contextGetOrForTest().owner
    config.path match {
      case "jobs" =>
        val infoMap = StreamingproJobManager.getJobInfo
        val spark = config.df.get.sparkSession
        val data = infoMap.toSeq.map(_._2).filter(_.owner == owner)
        import spark.implicits._
        spark.createDataset[StreamingproJobInfo](data).toDF()
    }

  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = SourceInfo(fullFormat, fullFormat, "jobs")

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "_mlsql_"

  override def shortFormat: String = "_mlsql_"
}

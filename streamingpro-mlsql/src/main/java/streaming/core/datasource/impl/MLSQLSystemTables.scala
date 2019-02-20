package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import streaming.core.StreamingproJobInfo
import streaming.core.datasource._
import streaming.core.datasource.util.MLSQLJobCollect
import streaming.dsl.ScriptSQLExec

/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLSystemTables extends MLSQLSource with MLSQLSourceInfo with MLSQLRegistry {

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val owner = ScriptSQLExec.contextGetOrForTest().owner
    val spark = config.df.get.sparkSession
    import spark.implicits._

    val jobCollect = new MLSQLJobCollect(spark, owner)
    config.path.split("/") match {
      case Array("jobs") =>
        spark.createDataset[StreamingproJobInfo](jobCollect.jobs).toDF()
      case Array("jobs", jobGroupId) =>
        spark.createDataset(Seq(jobCollect.jobDetail(jobGroupId))).toDF()
      case Array("progress", jobGroupId) =>
        spark.createDataset(jobCollect.jobProgress(jobGroupId)).toDF()
      case Array("resource") =>
        spark.createDataset(Seq(jobCollect.resourceSummary(null))).toDF()
      case Array("resource", jobGroupId) =>
        val detail = jobCollect.jobDetail(jobGroupId)
        detail.activeJobs.map(_.numActiveTasks)
        spark.createDataset(Seq(jobCollect.resourceSummary(jobGroupId))).toDF()
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

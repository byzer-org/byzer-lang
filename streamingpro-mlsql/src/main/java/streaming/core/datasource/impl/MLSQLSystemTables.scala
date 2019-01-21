package streaming.core.datasource.impl

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.{DataFrame, DataFrameReader, MLSQLUtils}
import org.apache.spark.status.api.v1
import streaming.core.datasource._
import streaming.core.output.protocal.{MLSQLScriptJob, MLSQLScriptJobGroup}
import streaming.core.{StreamingproJobInfo, StreamingproJobManager}
import streaming.dsl.ScriptSQLExec

import scala.collection.mutable.ListBuffer

/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLSystemTables extends MLSQLSource with MLSQLSourceInfo with MLSQLRegistry {

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val owner = ScriptSQLExec.contextGetOrForTest().owner
    val spark = config.df.get.sparkSession
    import spark.implicits._
    config.path.split("/") match {
      case Array("jobs") =>
        val infoMap = StreamingproJobManager.getJobInfo
        val data = infoMap.toSeq.map(_._2).filter(_.owner == owner)
        spark.createDataset[StreamingproJobInfo](data).toDF()
      case Array("jobs", jobGroupId) =>
        val store = MLSQLUtils.getAppStatusStore(spark)
        val appInfo = store.applicationInfo()
        val startTime = appInfo.attempts.head.startTime.getTime()
        val endTime = appInfo.attempts.head.endTime.getTime()

        val activeJobs = new ListBuffer[v1.JobData]()
        val completedJobs = new ListBuffer[v1.JobData]()
        val failedJobs = new ListBuffer[v1.JobData]()
        store.jobsList(null).filter(f => f.jobGroup.get == jobGroupId).foreach { job =>
          job.status match {
            case JobExecutionStatus.SUCCEEDED =>
              completedJobs += job
            case JobExecutionStatus.FAILED =>
              failedJobs += job
            case _ =>
              activeJobs += job
          }
        }

        val mlsqlActiveJobs = activeJobs.map { f =>

          MLSQLScriptJob(
            f.jobId,
            f.submissionTime.map(date => new java.sql.Date(date.getTime())),
            f.completionTime.map(date => new java.sql.Date(date.getTime())),
            f.numTasks,
            f.numActiveTasks,
            f.numCompletedTasks,
            f.numSkippedTasks,
            f.numFailedTasks,
            f.numKilledTasks,
            f.numCompletedIndices,
            f.numActiveTasks,
            f.numCompletedStages,
            f.numSkippedStages,
            f.numFailedStages
          )
        }

        spark.createDataset(Seq(MLSQLScriptJobGroup(
          jobGroupId, activeJobs.size, completedJobs.size, failedJobs.size, mlsqlActiveJobs
        ))).toDF()
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

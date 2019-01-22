package streaming.core.datasource.util

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.{MLSQLUtils, SparkSession}
import org.apache.spark.status.api.v1
import streaming.core.StreamingproJobManager
import streaming.core.output.protocal.{MLSQLResourceRender, MLSQLScriptJob, MLSQLScriptJobGroup}

import scala.collection.mutable.{Buffer, ListBuffer}

/**
  * 2019-01-22 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLJobCollect(spark: SparkSession, owner: String) {
  def jobs = {
    val infoMap = StreamingproJobManager.getJobInfo
    val data = infoMap.toSeq.map(_._2).filter(_.owner == owner)
    data
  }

  def getGroupId(jobNameOrGroupId: String) = {
    StreamingproJobManager.getJobInfo.filter(f => f._2.jobName == jobNameOrGroupId).headOption match {
      case Some(item) => item._2.groupId
      case None => jobNameOrGroupId
    }
  }

  def resourceSummary(jobGroupId: String) = {
    val store = MLSQLUtils.getAppStatusStore(spark)
    val executorList = store.executorList(true)
    val activeJobs = store.jobsList(null).filter(f => f.status == JobExecutionStatus.RUNNING)

    val finalJobGroupId = getGroupId(jobGroupId)

    def getNumActiveTaskByJob(job: v1.JobData) = {
      val (activeStages, completedStages, failedStages) = fetchStageByJob(job)
      activeStages.map(f => f.numActiveTasks).sum
    }

    val currentJobGroupActiveTasks = if (jobGroupId == null) activeJobs.map(getNumActiveTaskByJob).sum
    else activeJobs.filter(f => f.jobGroup.get == finalJobGroupId).map(getNumActiveTaskByJob).sum

    MLSQLResourceRender(
      currentJobGroupActiveTasks = currentJobGroupActiveTasks,
      activeTasks = executorList.map(_.activeTasks).sum,
      failedTasks = executorList.map(_.failedTasks).sum,
      completedTasks = executorList.map(_.completedTasks).sum,
      totalTasks = executorList.map(_.totalTasks).sum,
      taskTime = executorList.map(_.totalDuration).sum,
      gcTime = executorList.map(_.totalGCTime).sum,
      activeExecutorNum = executorList.size,
      totalCores = executorList.map(_.totalCores).sum
    )


  }

  def fetchStageByJob(f: v1.JobData) = {
    val store = MLSQLUtils.getAppStatusStore(spark)
    val stages = f.stageIds.map { stageId =>
      // This could be empty if the listener hasn't received information about the
      // stage or if the stage information has been garbage collected
      store.asOption(store.lastStageAttempt(stageId)).getOrElse {
        MLSQLUtils.createStage(stageId)
      }
    }

    val activeStages = Buffer[v1.StageData]()
    val completedStages = Buffer[v1.StageData]()
    // If the job is completed, then any pending stages are displayed as "skipped":
    val pendingOrSkippedStages = Buffer[v1.StageData]()
    val failedStages = Buffer[v1.StageData]()
    for (stage <- stages) {
      if (stage.submissionTime.isEmpty) {
        pendingOrSkippedStages += stage
      } else if (stage.completionTime.isDefined) {
        if (stage.status == v1.StageStatus.FAILED) {
          failedStages += stage
        } else {
          completedStages += stage
        }
      } else {
        activeStages += stage
      }
    }
    (activeStages, completedStages, failedStages)
  }

  def jobDetail(jobGroupId: String) = {
    val store = MLSQLUtils.getAppStatusStore(spark)
    val appInfo = store.applicationInfo()
    val startTime = appInfo.attempts.head.startTime.getTime()
    val endTime = appInfo.attempts.head.endTime.getTime()

    val finalJobGroupId = getGroupId(jobGroupId)

    val activeJobs = new ListBuffer[v1.JobData]()
    val completedJobs = new ListBuffer[v1.JobData]()
    val failedJobs = new ListBuffer[v1.JobData]()
    store.jobsList(null).filter(f => f.jobGroup.get == finalJobGroupId).foreach { job =>
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

      val (activeStages, completedStages, failedStages) = fetchStageByJob(f)

      MLSQLScriptJob(
        f.jobId,
        f.submissionTime.map(date => new java.sql.Date(date.getTime())),
        f.completionTime.map(date => new java.sql.Date(date.getTime())),
        f.numTasks,
        activeStages.map(f => f.numActiveTasks).sum,
        f.numCompletedTasks,
        f.numSkippedTasks,
        f.numFailedTasks,
        f.numKilledTasks,
        f.numCompletedIndices,
        f.numActiveStages,
        f.numCompletedStages,
        f.numSkippedStages,
        f.numFailedStages
      )
    }
    MLSQLScriptJobGroup(
      jobGroupId, activeJobs.size, completedJobs.size, failedJobs.size, mlsqlActiveJobs
    )
  }
}

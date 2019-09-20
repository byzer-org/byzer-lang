package org.apache.spark

import org.apache.spark.sql.{MLSQLUtils, SparkSession}
import org.apache.spark.status.api.v1
import tech.mlsql.render.protocal.{MLSQLResourceRender, MLSQLScriptJob, MLSQLScriptJobGroup, MLSQLShufflePerfRender}

import scala.collection.mutable.{Buffer, ListBuffer}

/**
  * 2019-01-28 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLResource(spark: SparkSession, owner: String, getGroupId: String => String) {

  def resourceSummary(jobGroupId: String) = {
    val store = MLSQLUtils.getAppStatusStore(spark)
    val executorList = store.executorList(true)
    val totalExecutorList = store.executorList(false)
    val activeJobs = store.jobsList(null).filter(f => f.status == JobExecutionStatus.RUNNING)

    val finalJobGroupId = getGroupId(jobGroupId)


    def getNumActiveTaskByJob(job: v1.JobData) = {
      val (activeStages, completedStages, failedStages) = fetchStageByJob(job)
      activeStages.map(f => f.numActiveTasks).sum
    }

    def getDiskBytesSpilled(job: v1.JobData) = {
      val (activeStages, completedStages, failedStages) = fetchStageByJob(job)
      activeStages.map(f => f.diskBytesSpilled).sum
    }

    def getInputRecords(job: v1.JobData) = {
      val (activeStages, completedStages, failedStages) = fetchStageByJob(job)
      activeStages.map(f => f.inputRecords).sum
    }

    def getMemoryBytesSpilled(job: v1.JobData) = {
      val (activeStages, completedStages, failedStages) = fetchStageByJob(job)
      activeStages.map(f => f.memoryBytesSpilled).sum
    }


    val currentJobGroupActiveTasks = if (jobGroupId == null) activeJobs.map(getNumActiveTaskByJob).sum
    else activeJobs.filter(f => f.jobGroup.get == finalJobGroupId).map(getNumActiveTaskByJob).sum

    val currentDiskBytesSpilled = if (jobGroupId == null) activeJobs.map(getDiskBytesSpilled).sum
    else activeJobs.filter(f => f.jobGroup.get == finalJobGroupId).map(getDiskBytesSpilled).sum

    val currentInputRecords = if (jobGroupId == null) activeJobs.map(getInputRecords).sum
    else activeJobs.filter(f => f.jobGroup.get == finalJobGroupId).map(getInputRecords).sum

    val currentMemoryBytesSpilled = if (jobGroupId == null) activeJobs.map(getMemoryBytesSpilled).sum
    else activeJobs.filter(f => f.jobGroup.get == finalJobGroupId).map(getMemoryBytesSpilled).sum

    val shuffle = MLSQLShufflePerfRender(memoryBytesSpilled = currentMemoryBytesSpilled, diskBytesSpilled = currentDiskBytesSpilled, inputRecords = currentInputRecords)

    MLSQLResourceRender(
      currentJobGroupActiveTasks = currentJobGroupActiveTasks,
      activeTasks = executorList.map(_.activeTasks).sum,
      failedTasks = executorList.map(_.failedTasks).sum,
      completedTasks = executorList.map(_.completedTasks).sum,
      totalTasks = executorList.map(_.totalTasks).sum,
      taskTime = executorList.map(_.totalDuration).sum,
      gcTime = executorList.map(_.totalGCTime).sum,
      activeExecutorNum = executorList.size,
      totalExecutorNum = totalExecutorList.size,
      totalCores = executorList.map(_.totalCores).sum,
      usedMemory = executorList.map(_.memoryUsed).sum,
      totalMemory = totalExecutorList.map(f => f.maxMemory).sum,
      shuffleData = shuffle
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
    store.jobsList(null).filter(f => f.jobGroup.isDefined).filter(f => f.jobGroup.get == finalJobGroupId).foreach { job =>
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

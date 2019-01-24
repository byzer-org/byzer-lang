package streaming.core.datasource.util

import org.apache.spark.sql.{MLSQLUtils, SparkSession}
import streaming.core.StreamingproJobManager
import streaming.core.output.protocal.{MLSQLResourceRender, MLSQLScriptJob, MLSQLScriptJobGroup}

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
    val store = MLSQLUtils.getExecutorAllocationManager(spark)
    val listener = MLSQLUtils.getAppStatusStore(spark)
    val executorList = store.executorToTaskSummary.values.toSeq
    val activeJobs = listener.activeJobs

    val finalJobGroupId = getGroupId(jobGroupId)

    def getNumActiveTaskByJob(stageIds: Set[Int]) = {
      val (activeStages, completedStages, failedStages) = fetchStageByJob(stageIds)
      activeStages.map(f => listener.stageIdToData(f.stageId, f.attemptId).numActiveTasks).sum
    }

    val currentJobGroupActiveTasks = if (jobGroupId == null) activeJobs.map { f =>
      getNumActiveTaskByJob(f._2.stageIds.toSet)
    }.sum
    else activeJobs.filter(f => f._2.jobGroup.get == finalJobGroupId).map { f =>
      getNumActiveTaskByJob(f._2.stageIds.toSet)
    }.sum

    MLSQLResourceRender(
      currentJobGroupActiveTasks = currentJobGroupActiveTasks,
      activeTasks = executorList.map(_.tasksActive).sum,
      failedTasks = executorList.map(_.tasksFailed).sum,
      completedTasks = executorList.map(_.tasksComplete).sum,
      totalTasks = executorList.map(_.tasksMax).sum,
      taskTime = executorList.map(_.duration).sum,
      gcTime = executorList.map(_.jvmGCTime).sum,
      activeExecutorNum = executorList.size,
      totalCores = executorList.map(_.totalCores).sum
    )


  }

  def fetchStageByJob(stageIds: Set[Int]) = {
    val listener = MLSQLUtils.getAppStatusStore(spark)
    val activeStages = listener.activeStages.values.filter(f => stageIds.contains(f.stageId)).toSeq
    val completedStages = listener.completedStages.filter(f => stageIds.contains(f.stageId)).reverse
    val failedStages = listener.failedStages.filter(f => stageIds.contains(f.stageId)).reverse
    (activeStages, completedStages, failedStages)
  }

  def jobDetail(jobGroupId: String) = {
    val listener = MLSQLUtils.getAppStatusStore(spark)
    val startTime = listener.startTime
    val endTime = listener.endTime

    val finalJobGroupId = getGroupId(jobGroupId)

    val activeJobs = listener.activeJobs.values.toSeq
    val completedJobs = listener.completedJobs.reverse
    val failedJobs = listener.failedJobs.reverse


    val mlsqlActiveJobs = activeJobs.map { f =>


      val (activeStages, completedStages, failedStages) = fetchStageByJob(f.stageIds.toSet)

      val activeTasks = activeStages.map(f => listener.stageIdToData(f.stageId, f.attemptId).numActiveTasks).sum

      MLSQLScriptJob(
        f.jobId,
        f.submissionTime.map(date => new java.sql.Date(date)),
        f.completionTime.map(date => new java.sql.Date(date)),
        f.numTasks,
        f.numActiveTasks,
        f.numCompletedTasks,
        f.numSkippedTasks,
        f.numFailedTasks,
        0,
        0,
        f.numActiveStages,
        0,
        f.numSkippedStages,
        f.numFailedStages
      )
    }
    MLSQLScriptJobGroup(
      jobGroupId, activeJobs.size, completedJobs.size, failedJobs.size, mlsqlActiveJobs
    )
  }
}

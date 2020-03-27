package org.apache.spark

import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.JobManager
import tech.mlsql.plugins.mlsql_watcher.db.{WExecutor, WExecutorJob}

import scala.collection.mutable

/**
 * 10/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object DataCompute extends Logging {
  def runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]

  def compute() = {
    val appName = runtime.sparkSession.sparkContext.getConf.get("spark.app.name")
    val computeTime = System.currentTimeMillis()
    val statusStore = runtime.sparkSession.sparkContext.statusStore
    val executorItems = statusStore.executorList(false).map { item =>
      WExecutor(
        id = 0,
        appName,
        name = item.id.toString,
        hostPort = item.hostPort,
        totalShuffleRead = item.totalShuffleRead,
        totalShuffleWrite = item.totalShuffleWrite,
        gcTime = item.totalGCTime,
        addTime = item.addTime.getTime,
        removeTime = item.removeTime.map(_.getTime).getOrElse(-1L),
        createdAt = computeTime
      )
    }.toList

    val executorMap = new mutable.HashMap[ExecutorGroupKey, WExecutorJob]()

    val runningGroupIds = JobManager.getJobInfo.map(_._2.groupId).toSet

    statusStore.jobsList(new java.util.ArrayList[JobExecutionStatus]()).foreach { item =>
      val groupId = item.jobGroup.getOrElse("NONE")

      if(runningGroupIds.contains(groupId)){
        item.stageIds.foreach { id =>
          statusStore.stageData(id).foreach {
            stage =>
              //stage.attemptId
              val tasks = statusStore.taskList(stage.stageId, stage.attemptId, Integer.MAX_VALUE)
              tasks.filter(_.taskMetrics.isDefined).foreach { task =>
                val taskData = task
                val _diskBytesSpilled = taskData.taskMetrics.get.diskBytesSpilled
                val _shuffleRemoteBytesRead = taskData.taskMetrics.get.shuffleReadMetrics.remoteBytesRead
                val _shuffleLocalBytesRead = taskData.taskMetrics.get.shuffleReadMetrics.localBytesRead
                val _shuffleRecordsRead = taskData.taskMetrics.get.shuffleReadMetrics.recordsRead
                val _shuffleBytesWritten = taskData.taskMetrics.get.shuffleWriteMetrics.bytesWritten
                val _shuffleRecordsWritten = taskData.taskMetrics.get.shuffleWriteMetrics.recordsWritten


                if (!executorMap.contains(ExecutorGroupKey(taskData.executorId, groupId))) {
                  executorMap.put(ExecutorGroupKey(taskData.executorId, groupId), WExecutorJob(0, appName, groupId,
                    taskData.executorId,
                    _diskBytesSpilled,
                    _shuffleRemoteBytesRead,
                    _shuffleLocalBytesRead,
                    _shuffleRecordsRead,
                    _shuffleBytesWritten,
                    _shuffleRecordsWritten, 0L, 0L, computeTime

                  ))
                } else {
                  val wej = executorMap(ExecutorGroupKey(taskData.executorId, groupId))

                  val newwej = wej.copy(diskBytesSpilled = (wej.diskBytesSpilled + _diskBytesSpilled),
                    shuffleRemoteBytesRead = (wej.shuffleRemoteBytesRead + _shuffleRemoteBytesRead),
                    shuffleLocalBytesRead = (wej.shuffleLocalBytesRead + _shuffleLocalBytesRead),
                    shuffleRecordsRead = (wej.shuffleRecordsRead + _shuffleRecordsRead),
                    shuffleBytesWritten = (wej.shuffleBytesWritten + _shuffleBytesWritten),
                    shuffleRecordsWritten = (wej.shuffleRecordsWritten + _shuffleRecordsWritten))

                  executorMap.put(ExecutorGroupKey(taskData.executorId, groupId), newwej)
                }
              }
          }

        }
      }
    }


    (executorItems, executorMap.map(_._2).toList)
  }
}

case class ExecutorGroupKey(executorId: String, groupId: String)

case class GroupKey(groupId: String)

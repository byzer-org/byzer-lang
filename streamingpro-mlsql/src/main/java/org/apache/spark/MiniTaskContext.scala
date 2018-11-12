package org.apache.spark

import java.util.Properties

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryManager, TaskMemoryManager}
import org.apache.spark.metrics.source.Source
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener}

class MiniTaskContext extends TaskContext {
  val memoryManager: MemoryManager = SparkEnv.get.memoryManager

  private var complete = false

  override def isCompleted(): Boolean = complete

  override def isInterrupted(): Boolean = false

  override def isRunningLocally(): Boolean = true

  override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext = {
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): TaskContext = {
    this
  }

  override def stageId(): Int = 0

  override def stageAttemptNumber(): Int = 0

  override def partitionId(): Int = 0

  override def attemptNumber(): Int = 0

  override def taskAttemptId(): Long = 0

  override def getLocalProperty(key: String): String = {
    key
  }

  override def taskMetrics(): TaskMetrics = new TaskMetrics

  override def getMetricsSources(sourceName: String): Seq[Source] = Seq.empty

  override private[spark] def killTaskIfInterrupted(): Unit = {}

  override private[spark] def getKillReason() = None

  override private[spark] def taskMemoryManager() = new TaskMemoryManager(memoryManager, 0)

  override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {

  }

  override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = {

  }

  TaskContext.setTaskContext(this)

  def removeContext(): Unit = {
    TaskContext.unset()
  }

  override private[spark] def markInterrupted(reason: String): Unit = {}

  override private[spark] def markTaskFailed(error: Throwable): Unit = {}

  override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = {}

  override private[spark] def fetchFailed = None

  override private[spark] def getLocalProperties = {
    new Properties()
  }
}

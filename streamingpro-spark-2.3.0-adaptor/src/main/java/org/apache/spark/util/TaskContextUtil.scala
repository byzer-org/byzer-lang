package org.apache.spark.util

import org.apache.spark.{TaskContext, TaskContextImpl}

/**
  * Created by allwefantasy on 16/7/2018.
  */
object TaskContextUtil {
  def markInterrupted(context: TaskContext, e: Throwable) = {
    context.asInstanceOf[TaskContextImpl].markInterrupted(e.getMessage)
  }

  def markTaskCompleted(context: TaskContext, e: Throwable) = {
    context.asInstanceOf[TaskContextImpl].markTaskCompleted(Some(e))
  }

  def setContext(context:TaskContext) = {
    TaskContext.setTaskContext(context)
  }
}

object PredictTaskContext {
  /**
    * Return the currently active TaskContext. This can be called inside of
    * user functions to access contextual information about running tasks.
    */
  def get(): TaskContext = taskContext.get

  /**
    * Returns the partition id of currently active TaskContext. It will return 0
    * if there is no active TaskContext for cases like local execution.
    */
  def getPartitionId(): Int = {
    val tc = taskContext.get()
    if (tc eq null) {
      0
    } else {
      tc.partitionId()
    }
  }

  private[this] val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  // Note: protected[spark] instead of private[spark] to prevent the following two from
  // showing up in JavaDoc.
  /**
    * Set the thread local TaskContext. Internal to Spark.
    */
  def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

  /**
    * Unset the thread local TaskContext. Internal to Spark.
    */
  def unset(): Unit = taskContext.remove()


}
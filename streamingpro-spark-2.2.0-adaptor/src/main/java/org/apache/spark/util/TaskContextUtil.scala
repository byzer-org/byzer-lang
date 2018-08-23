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

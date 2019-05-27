package org.apache.spark.scheduler.cluster

import org.apache.spark.ExecutorAllocationClient
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 2019-04-26 WilliamZhu(allwefantasy@gmail.com)
  */
class SparkInnerExecutors(session: SparkSession) {
  def executorCores = {
    val items = executorDataMap
    if (items.size > 0) {
      items.head._2.totalCores
    } else {
      java.lang.Runtime.getRuntime.availableProcessors
    }
  }

  def executorDataMap = {
    executorAllocationClient match {
      case Some(eac) =>
        val item = eac.asInstanceOf[CoarseGrainedSchedulerBackend]
        val field = classOf[CoarseGrainedSchedulerBackend].
          getDeclaredField("org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
        field.setAccessible(true)
        val executors = field.get(item).asInstanceOf[mutable.Map[String, ExecutorData]]
        executors
      case None => Map[String, ExecutorData]()
    }
  }

  def executorMemory = {
    session.sparkContext.executorMemory
  }

  def executorAllocationClient = {
    session.sparkContext.schedulerBackend match {
      case sb if sb.isInstanceOf[CoarseGrainedSchedulerBackend] =>
        Option(sb.asInstanceOf[ExecutorAllocationClient])
      case sb if sb.isInstanceOf[LocalSchedulerBackend] =>
        None
    }
  }

  def status: ResourceStatus = {
    val totalCores = executorDataMap.map(f => f._2.totalCores).sum
    val totalMemory = executorDataMap.map(f => executorMemory).sum
    val executorNum = executorDataMap.size
    ResourceStatus(totalCores, totalMemory, executorNum)
  }
}

case class ResourceStatus(totalCores: Int, totalMemory: Long, executorNum: Int)

class SparkDynamicControlExecutors(session: SparkSession) {
  private[this] val sparkInnerExecutors = new SparkInnerExecutors(session)

  private def changeExecutors(num: Int, timeout: Long, isAdd: Boolean, f: () => Unit) = {
    val currentSize = sparkInnerExecutors.executorDataMap.size
    val targetSize = if (isAdd) num else currentSize - num
    f()
    var count = 0
    var susscess = false
    while (!susscess && count < timeout / 1000) {
      val _currentSize = sparkInnerExecutors.executorDataMap.size
      susscess = (_currentSize == targetSize)
      Thread.sleep(1000)
      count += 1
    }
    if (count >= timeout / 1000) {
      throw new RuntimeException(
        s"""
           |Resource Info:
           |
           |current_executor_num: ${currentSize}
           |target_executor_num: ${targetSize}
           |
           |Please check the status manually, maybe the cluster is too busy and we can not
           |allocate/deallocate executors.
        """.stripMargin)
    }
  }

  def requestTotalExecutors(num: Int, timeout: Long) = {
    changeExecutors(num, timeout, true, () => {
      session.sparkContext.requestTotalExecutors(num, 0, Map.empty)
    })
  }

  def killExecutors(num: Int, timeout: Long) = {
    val items = sparkInnerExecutors.executorDataMap.keys.take(num)
    changeExecutors(num, timeout, false, () => {
      session.sparkContext.killExecutors(items.toSeq)
    })

  }
}


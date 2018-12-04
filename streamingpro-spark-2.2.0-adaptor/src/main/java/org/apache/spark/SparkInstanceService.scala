package org.apache.spark

import org.apache.spark.sql.SparkSession

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
class SparkInstanceService(session: SparkSession) {

  def resources = {
    var totalTasks = 0l
    var totalUsedMemory = 0l
    var totalMemory = 0l
    session.sparkContext.statusTracker.getExecutorInfos.map { worker =>
      totalTasks += worker.numRunningTasks()
      totalUsedMemory += -1
      totalMemory += -1

    }
    SparkInstanceResource(totalTasks, totalUsedMemory, totalMemory)
  }
}

case class SparkInstanceResource(totalTasks: Long, totalUsedMemory: Long, totalMemory: Long)

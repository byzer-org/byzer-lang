package org.apache.spark

import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

/**
  * 2019-04-26 WilliamZhu(allwefantasy@gmail.com)
  */
object Main {
  def main(args: Array[String]): Unit = {
    classOf[CoarseGrainedSchedulerBackend].getDeclaredFields().foreach(f => println(f.getName))
    val field = classOf[CoarseGrainedSchedulerBackend].getDeclaredField("org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
    println(field)

  }
}



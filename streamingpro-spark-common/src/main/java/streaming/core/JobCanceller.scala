package streaming.core

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j.Logger
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 15/5/2017.
  */
object JobCanceller {
  val logger = Logger.getLogger(classOf[JobCanceller])
  var jobCanceller: JobCanceller = _

  def init(sc: SparkContext, initialDelay: Long = 30, checkTimeInterval: Long = 5) = {
    synchronized {
      if (jobCanceller == null) {
        logger.info(s"JobCanceller Timer  started with initialDelay=${initialDelay} checkTimeInterval=${checkTimeInterval}")
        jobCanceller = new JobCanceller(sc, initialDelay, checkTimeInterval)
        jobCanceller.run
      }
    }
  }

  def runWithGroup(sc: SparkContext, timeout: Long, f: () => Unit) = {
    if (jobCanceller == null) {
      f()
    } else {
      val groupId = jobCanceller.nextGroupId.incrementAndGet().toString
      sc.setJobGroup(groupId, "", true)
      try {
        jobCanceller.groupIdToTime.put(groupId, JobTime(System.currentTimeMillis(), timeout))
        f()
      }
      finally {
        sc.clearJobGroup()
      }
    }
  }
}

class JobCanceller(sc: SparkContext, initialDelay: Long, checkTimeInterval: Long) {
  val groupIdToTime = new java.util.concurrent.ConcurrentHashMap[String, JobTime]()
  val nextGroupId = new AtomicInteger(0)
  val logger = Logger.getLogger(classOf[JobCanceller])
  val executor = Executors.newSingleThreadScheduledExecutor()

  def run = {
    executor.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        val items = new ArrayBuffer[String]()
        groupIdToTime.foreach { f =>
          val elapseTime = System.currentTimeMillis() - f._2.startTime
          if (elapseTime >= f._2.timeout) {
            items += f._1
            cancelJobGroup(f._1)
          }
        }

        items.foreach(f => groupIdToTime.remove(f))
      }
    }, initialDelay, checkTimeInterval, TimeUnit.SECONDS)
  }

  def cancelJobGroup(groupId: String): Unit = {
    logger.info("JobCanceller Timer cancel job group " + groupId)
    sc.cancelJobGroup(groupId)
  }
}

case class JobTime(startTime: Long, timeout: Long)

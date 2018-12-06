package tech.mlsql.cluster.service.elastic_resource

import java.util.concurrent.{Executors, TimeUnit}
import java.util.logging.Logger

import tech.mlsql.cluster.model.ElasticMonitor

import scala.collection.JavaConverters._

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
object AllocateService {
  val logger = Logger.getLogger("AllocateService")
  private[this] val _executor = Executors.newFixedThreadPool(100)
  private[this] val scheduler = Executors.newSingleThreadScheduledExecutor()

  private[this] val mapping = Map(
    "JobNumAwareAllocate" -> new JobNumAwareAllocateStrategy()
  )

  def shutdown = {
    _executor.shutdownNow()
  }

  def run = {
    scheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        ElasticMonitor.items().asScala.foreach { monitor =>
          try {
            val allocate = mapping(monitor.getAllocateStrategy).allocate(monitor.getTag.split(",").toSeq, monitor.getAllocateType)
            allocate match {
              case Some(item) =>
                _executor.execute(new Runnable {
                  override def run(): Unit = {
                    //do the allocate
                  }
                })
              case None =>
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
            //catch all ,so the scheduler will not been stopped by the exception
          }
        }
      }
    }, 60, 10, TimeUnit.SECONDS)
  }
}



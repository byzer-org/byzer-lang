package tech.mlsql.cluster.service.elastic_resource

import java.util.concurrent.{Executors, TimeUnit}
import java.util.logging.Logger

import streaming.log.Logging
import tech.mlsql.cluster.ProxyApplication
import tech.mlsql.cluster.model.ElasticMonitor

import scala.collection.JavaConverters._

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
object AllocateService extends Logging {
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
            val allocateStrategy = mapping(monitor.getAllocateStrategy)
            val tags = monitor.getTag.split(",").toSeq
            val allocate = allocateStrategy.plan(tags, monitor.getAllocateType)

            allocate match {
              case Some(item) =>
                _executor.execute(new Runnable {
                  override def run(): Unit = {
                    logDebug(s"check instances tagged with [$tags]. Allocate ${item}")
                    allocateStrategy.allocate(item, monitor.getAllocateType)
                  }
                })
              case None =>
                logDebug(s"check instances tagged with [$tags]. Allocate None")
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
            //catch all ,so the scheduler will not been stopped by  exception
          }
        }
      }
    }, 60, ProxyApplication.commandConfig.allocateCheckInterval, TimeUnit.SECONDS)
  }
}



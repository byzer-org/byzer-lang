package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.sql.util.ExecutionListenerBus
import tech.mlsql.common.utils.lang.sc.ScalaReflect

import scala.collection.JavaConverters._

/**
 * 13/1/2021 WilliamZhu(allwefantasy@gmail.com)
 */
object CleanerUtils {
  def listenerBus(sparkContext: SparkContext) = {
    sparkContext.listenerBus
  }

  def listeners(bus: LiveListenerBus) = {
    bus.listeners
  }

  def filterExecutionListenerBusWithSession(bus: LiveListenerBus, session: SparkSession) = {
    val listeners = bus.listeners.asScala.filter(_.isInstanceOf[ExecutionListenerBus]).
      map(_.asInstanceOf[ExecutionListenerBus]).
      filter { item =>
        ScalaReflect.fromInstance[ExecutionListenerBus](item).field("session").invoke() == session
      }
    listeners
  }
}

package org.apache.spark.api

import org.apache.spark.ExecutorPlugin

/**
 * 25/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait MLSQLExecutorPlugin extends ExecutorPlugin {
  override def init(): Unit = _init(Map[Any, Any]())

  override def shutdown(): Unit = _shutdown()

  def _init(config: Map[Any, Any]): Unit

  def _shutdown(): Unit
}

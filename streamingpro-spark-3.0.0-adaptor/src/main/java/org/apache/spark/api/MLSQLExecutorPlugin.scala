package org.apache.spark.api

import org.apache.spark.ExecutorPlugin

/**
 * The parameters in init method are different in different version of Spark.
 * We make sure finally the implementation can hold all parameters, and we can handle the
 * differences. 
 */
trait MLSQLExecutorPlugin extends ExecutorPlugin {
  override def init(): Unit = _init(Map[Any, Any]())

  override def shutdown(): Unit = _shutdown()

  def _init(config: Map[Any, Any]): Unit

  def _shutdown(): Unit
}


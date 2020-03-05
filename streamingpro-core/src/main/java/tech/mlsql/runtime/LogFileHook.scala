package tech.mlsql.runtime

import streaming.core.strategy.platform.StreamingRuntime


/**
 * 2019-09-12 WilliamZhu(allwefantasy@gmail.com)
 */
class LogFileHook extends MLSQLPlatformLifecycle {

  override def beforeDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}

  override def afterDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}

  override def beforeRuntime(params: Map[String, String]): Unit = {

  }

  override def afterRuntime(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}
}

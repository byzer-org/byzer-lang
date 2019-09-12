package tech.mlsql.runtime

import streaming.core.strategy.platform.StreamingRuntime
import tech.mlsql.common.utils.shell.command.ParamsUtil

/**
  * 2019-09-12 WilliamZhu(allwefantasy@gmail.com)
  */
class LogFileHook extends MLSQLPlatformLifecycle {

  override def beforeDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}

  override def afterDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}

  override def beforeRuntime(params: Map[String, String]): Unit = {
    def configureLogProperty(params: ParamsUtil) = {
      if (System.getProperty("REALTIME_LOG_HOME") == null) {
        System.setProperty("REALTIME_LOG_HOME", params.getParam("REALTIME_LOG_HOME",
          "/tmp/__mlsql__/logs"))
      }
    }
  }

  override def afterRuntime(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}
}

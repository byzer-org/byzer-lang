package tech.mlsql.runtime

import streaming.core.strategy.platform.{SparkRuntime, StreamingRuntime}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.includes.PluginIncludeSource
import tech.mlsql.ets._

/**
  * 2019-09-12 WilliamZhu(allwefantasy@gmail.com)
  */
class PluginHook extends MLSQLPlatformLifecycle with Logging {

  override def beforeRuntime(params: Map[String, String]): Unit = {}

  override def afterRuntime(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}

  override def beforeDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {
    if (!params.contains("streaming.datalake.path")) return
    val spark = runtime.asInstanceOf[SparkRuntime].sparkSession


    import PluginCommand._
    import SchedulerCommand._
    import spark.implicits._


    val plugins = tryReadTable(spark, TABLE_PLUGINS, () => spark.createDataset[AddPlugin](Seq()).toDF())
    val ets = tryReadTable(spark, TABLE_ETRecord, () => spark.createDataset[ETRecord](Seq()).toDF())
    val dses = tryReadTable(spark, TABLE_DSRecord, () => spark.createDataset[DSRecord](Seq()).toDF())
    val apps = tryReadTable(spark, TABLE_APPRecord, () => spark.createDataset[AppRecord](Seq()).toDF())

    plugins.as[AddPlugin].collect().foreach { plugin =>
      logInfo(s"Plugin ${plugin.pluginName} in ${plugin.path}")
      val localPath = downloadFromHDFS(plugin.path.split("/").last, plugin.path)

      if (plugin.pluginType == PluginType.DS || plugin.pluginType == PluginType.ET) {
        loadJarInDriver(localPath)
        spark.sparkContext.addJar(localPath)
      }

      if (plugin.pluginType == PluginType.SCRIPT) {
        PluginIncludeSource.register(plugin.pluginName, plugin.path)
      }
    }

    ets.as[ETRecord].collect().foreach { et =>
      logInfo(s"Register ET Plugin ${et.pluginName} in ${et.className}")
      registerET(et.pluginName, et.className, et.commandName, () => {
      })
    }

    dses.as[DSRecord].collect().foreach { ds =>
      logInfo(s"Register DS Plugin ${ds.pluginName} in ${ds.fullFormat}")
      registerDS(ds.pluginName, ds.fullFormat, ds.shortFormat, () => {
      })
    }

    apps.as[AppRecord].collect().foreach { ds =>
      logInfo(s"Register App Plugin ${ds.pluginName} in ${ds.className}")
      Class.forName(ds.className).newInstance().asInstanceOf[tech.mlsql.app.App].run(ds.params)
    }

  }

  override def afterDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {

  }
}

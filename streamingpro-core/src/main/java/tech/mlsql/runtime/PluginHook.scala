package tech.mlsql.runtime

import org.apache.spark.SparkCoreVersion
import streaming.core.strategy.platform.{SparkRuntime, StreamingRuntime}
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.dsl.includes.PluginIncludeSource
import tech.mlsql.runtime.plugins._
import tech.mlsql.store.DBStore

/**
 * 2019-09-12 WilliamZhu(allwefantasy@gmail.com)
 */
class PluginHook extends MLSQLPlatformLifecycle with Logging {

  override def beforeRuntime(params: Map[String, String]): Unit = {}

  override def afterRuntime(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}

  override def beforeDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {
    // build-in plugins
    params.getOrElse("streaming.plugin.buildin.loads.before", "").
      split(",").filterNot(_.isEmpty).
      foreach { beforeItem =>
        AppRuntimeStore.store.registerLoadSave(AppRuntimeStore.LOAD_BEFORE_KEY, beforeItem)
      }

    params.getOrElse("streaming.plugin.buildin.loads.after", "tech.mlsql.plugin.load.DefaultLoaderPlugin").split(",").filterNot(_.isEmpty).
      foreach { afterItem =>
        AppRuntimeStore.store.registerLoadSave(AppRuntimeStore.LOAD_AFTER_KEY, afterItem)
      }

    // build-in plugins
    PluginHook.startBuildIn(
      params.get("streaming.plugin.clzznames").
        map(item => item.split(",")).
        getOrElse(Array[String]()))

    if (!params.contains("streaming.datalake.path")) return
    val spark = runtime.asInstanceOf[SparkRuntime].sparkSession
    import PluginUtils._
    import spark.implicits._

    val plugins = DBStore.store.tryReadTable(spark, TABLE_PLUGINS, () => spark.createDataset[AddPlugin](Seq()).toDF())
    val ets = DBStore.store.tryReadTable(spark, TABLE_ETRecord, () => spark.createDataset[ETRecord](Seq()).toDF())
    val dses = DBStore.store.tryReadTable(spark, TABLE_DSRecord, () => spark.createDataset[DSRecord](Seq()).toDF())
    val apps = DBStore.store.tryReadTable(spark, TABLE_APPRecord, () => spark.createDataset[AppRecord](Seq()).toDF())

    plugins.as[AddPlugin].collect().foreach { plugin =>
      logInfo(s"Plugin ${plugin.pluginName} in ${plugin.path}")
      val localPath = downloadFromHDFSToLocal(plugin.path.split(PathFun.pathSeparator).last, plugin.path, spark.sparkContext.hadoopConfiguration)

      if (plugin.pluginType == PluginType.DS
        || plugin.pluginType == PluginType.ET
        || plugin.pluginType == PluginType.APP
      ) {
        loadJarInDriver(localPath)
        spark.sparkContext.addJar(localPath)
      }

      if (plugin.pluginType == PluginType.SCRIPT) {
        PluginIncludeSource.register(plugin.pluginName, localPath)
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
      ClassLoaderTool.classForName(ds.className).newInstance().asInstanceOf[tech.mlsql.app.App].run(ds.params)
    }

  }

  override def afterDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {

  }
}

object PluginHook extends Logging {
  private val apps = List(
    "tech.mlsql.plugins.app.pythoncontroller.PythonApp",
    "tech.mlsql.plugins.mlsql_watcher.MLSQLWatcher",
    "tech.mlsql.plugins.sql.profiler.ProfilerApp",
    "tech.mlsql.autosuggest.app.MLSQLAutoSuggestApp",
    "tech.mlsql.plugins.ets.ETApp",
    "tech.mlsql.plugins.healthy.App"
  )

  def startBuildIn(extra: Array[String]): Unit = {
    (apps ++ extra).foreach { app =>
      try {
        ClassLoaderTool.classForName(app).newInstance().
          asInstanceOf[tech.mlsql.app.App].run(Seq[String]())
      } catch {
        case e: Exception =>
          logInfo("Fail to start default plugin", e)
      }
    }

  }
}

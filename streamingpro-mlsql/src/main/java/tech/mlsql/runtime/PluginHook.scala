package tech.mlsql.runtime

import org.apache.spark.sql.DataFrame
import streaming.core.strategy.platform.{SparkRuntime, StreamingRuntime}
import tech.mlsql.common.utils.log.Logging
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

    def tryReadTable(table: String, empty: () => DataFrame) = {
      try {
        readTable(spark, table)
      } catch {
        case e: Exception =>
          empty()
      }
    }

    val plugins = tryReadTable(TABLE_PLUGINS, () => spark.createDataset[AddPlugin](Seq()).toDF())
    val ets = tryReadTable(TABLE_ETRecord, () => spark.createDataset[ETRecord](Seq()).toDF())
    val dses = tryReadTable(TABLE_DSRecord, () => spark.createDataset[DSRecord](Seq()).toDF())

    plugins.as[AddPlugin].collect().foreach { plugin =>
      logInfo(s"Plugin ${plugin.pluginName} in ${plugin.path}")
      val localPath = downloadFromHDFS(plugin.path.split("/").last, plugin.path)
      loadJarInDriver(localPath)
      spark.sparkContext.addJar(localPath)
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

  }

  override def afterDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {

  }
}

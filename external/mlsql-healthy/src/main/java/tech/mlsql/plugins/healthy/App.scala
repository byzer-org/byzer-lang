package tech.mlsql.plugins.healthy

import streaming.dsl.ScriptSQLExec
import tech.mlsql.app.CustomController
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.version.VersionCompatibility

/**
 * 23/9/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class App extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AppRuntimeStore.store.registerController("/plugins/healthy", classOf[HealthyController].getName)
    AppRuntimeStore.store.registerRequestCleaner("/plugins/cleaner/healthy",classOf[ShutdownOnSparkContextStoppedCleaner].getName)
    AppRuntimeStore.store.registerRequestCleaner("/plugins/cleaner/clean/temp_tables",classOf[TempTableCleaner].getName)
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}

class HealthyController extends CustomController {
  override def run(params: Map[String, String]): String = {
    params.get("_action_") match {
      case Some(action) =>
        matchAction(action)

      case None =>
        matchAction("info")
    }

  }

  def matchAction(action: String): String = {
    val context = ScriptSQLExec.context()
    val session = context.execListener.sparkSession
    action match {
      case "info" =>
        JSONTool.toJsonStr(Map("sparkContextIsStopped" -> session.sparkContext.isStopped))
      case "shutdown" =>

        val t = new Thread(new Runnable {
          override def run(): Unit = {
            Thread.sleep(30 * 1000)
            if (!session.sparkContext.isStopped) {
              session.sparkContext.stop()
            }
            System.exit(-1)
          }
        })
        t.start()
        JSONTool.toJsonStr(Map("success" -> "30 seconds later we will shutdown"))
    }
  }
}

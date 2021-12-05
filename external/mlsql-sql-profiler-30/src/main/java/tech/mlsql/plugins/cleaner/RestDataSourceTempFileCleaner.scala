package tech.mlsql.plugins.cleaner

import streaming.dsl.ScriptSQLExec
import streaming.log.WowLog
import tech.mlsql.app.RequestCleaner
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.tool.HDFSOperatorV2

/**
 * 2/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class RestDataSourceTempFileCleaner extends RequestCleaner with Logging with WowLog {

  val NAME = "tech.mlsql.datasource.impl.MLSQLRest"

  override def run(): Unit = {
    val context = ScriptSQLExec.context()

    val shouldCleanTempData = context.execListener.env().getOrElse("enableRestDataSourceRequestCleaner", "false").toBoolean
    if (!shouldCleanTempData) {
      return
    }

    val ifAsync = JSONTool.parseJson[Map[String, String]](context.userDefinedParam("__PARAMS__")).getOrElse("async", "false").toBoolean
    if (ifAsync && !context.execListener.env().getOrElse("__MarkAsyncRunFinish__", "false").toBoolean) {
      return
    }

    context.execListener.env().get(NAME) match {
      case Some(dirs) =>
        dirs.split(",").foreach { dir =>
          logInfo(s"${NAME}: clean ${dir}")
          try {
            HDFSOperatorV2.deleteDir(dir)
          } catch {
            case e: Exception =>
              logInfo(s"Fail to clean ${dir}", e)
          }

        }
      case None =>
    }
  }
}

package tech.mlsql.plugins.sql.profiler

import org.apache.spark.sql.catalyst.sqlgenerator.{BasicSQLDialect, LogicalPlanSQL}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.app.CustomController
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.adaptor.{LoadStatement, SelectStatement}
import tech.mlsql.job.{JobManager, MLSQLJobInfo}
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
 * 2/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class GenSQLController extends CustomController {
  override def run(params: Map[String, String]): String = {

    try {
      ViewCatalyst.createViewCatalyst()
      val context = ScriptSQLExec.context()
      val session = context.execListener.sparkSession
      val jobInfo = JSONTool.parseJson[MLSQLJobInfo](params("__jobinfo__"))

      val sql = params("sql")
      // 解析load语句

      ScriptSQLExec.parse(sql, context.execListener,
        skipInclude = false,
        skipAuth = true,
        skipPhysicalJob = true,
        skipGrammarValidate = true)

      val stats = context.execListener.preProcessListener.map(f => f.analyzedStatements.map(_.unwrap))
      stats.get.map { singleSt => {
        singleSt match {
          case a: LoadStatement =>
            val LoadStatement(_, format, path, option, tableName) = a
            //            new LoadPRocessing(context.execListener, option, path, tableName, format).parse
            //            val view = session.table(tableName).queryExecution.analyzed
            //            val newview = view match {
            //              case SubqueryAlias(alias, child) => child
            //              case _ => view
            //            }
            ViewCatalyst.meta.register(tableName, path, format)
          case _: SelectStatement =>
          case None => throw new RuntimeException("Only load/select are supported in gen sql interface")

        }
      }
      }

      var temp = ""
      JobManager.run(session, jobInfo, () => {
        ScriptSQLExec.parse(sql, context.execListener,
          skipInclude = params.getOrElse("skipInclude", "false").toBoolean,
          skipAuth = params.getOrElse("skipAuth", "true").toBoolean,
          skipPhysicalJob = params.getOrElse("skipPhysicalJob", "false").toBoolean,
          skipGrammarValidate = params.getOrElse("skipGrammarValidate", "true").toBoolean
        )
        context.execListener.getLastSelectTable() match {
          case Some(tableName) =>
            val lp = session.table(tableName).queryExecution.analyzed
            temp = new LogicalPlanSQL(lp, new BasicSQLDialect).toSQL
          case None =>
        }
      })
      temp
    }
    finally {
      ViewCatalyst.unset
    }

  }
}

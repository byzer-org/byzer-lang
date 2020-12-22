package tech.mlsql.tool

import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.adaptor.{LoadStatement, SelectStatement}
import tech.mlsql.job.{JobManager, MLSQLJobInfo}
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
 * 17/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLAnalyzer(params: Map[String, String]) {
  private val sql = params("sql")
  private val jobInfo = JSONTool.parseJson[MLSQLJobInfo](params("__jobinfo__"))
  private val context = ScriptSQLExec.context()
  private val session = context.execListener.sparkSession

  def getSession = {
    session
  }

  def analyze = {
    ScriptSQLExec.parse(sql, context.execListener,
      skipInclude = false,
      skipAuth = true,
      skipPhysicalJob = true,
      skipGrammarValidate = true)
    val stats = context.execListener.preProcessListener.map(f => f.analyzedStatements.map(_.unwrap))
    stats.map(_.toList)
  }

  def extractLoads: Unit = {
    analyze.get.foreach { singleSt => {
      singleSt match {
        case a: LoadStatement =>
          val LoadStatement(_, format, path, option, tableName) = a
          //            new LoadPRocessing(context.execListener, option, path, tableName, format).parse
          //            val view = session.table(tableName).queryExecution.analyzed
          //            val newview = view match {
          //              case SubqueryAlias(alias, child) => child
          //              case _ => view
          //            }
          ViewCatalyst.meta.register(tableName, path, format,option)
        case _: SelectStatement =>
        case None => throw new RuntimeException("Only load/select are supported in gen sql interface")

      }
    }
    }
  }

  def executeAndGetLastTable() = {
    var temp: Option[String] = None
    JobManager.run(session, jobInfo, () => {
      ScriptSQLExec.parse(sql, context.execListener,
        skipInclude = params.getOrElse("skipInclude", "false").toBoolean,
        skipAuth = params.getOrElse("skipAuth", "true").toBoolean,
        skipPhysicalJob = params.getOrElse("skipPhysicalJob", "false").toBoolean,
        skipGrammarValidate = params.getOrElse("skipGrammarValidate", "true").toBoolean
      )
      context.execListener.getLastSelectTable() match {
        case Some(tableName) =>
          temp = Some(tableName)
        case None =>
      }
    })
    temp
  }
}

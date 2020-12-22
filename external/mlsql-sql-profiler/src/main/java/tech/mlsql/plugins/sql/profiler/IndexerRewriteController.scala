package tech.mlsql.plugins.sql.profiler

import org.apache.spark.sql.catalyst.sqlgenerator.LogicalPlanSQL
import streaming.dsl.ScriptSQLExec
import tech.mlsql.app.CustomController
import tech.mlsql.dsl.adaptor.{LoadStatement, SelectStatement}
import tech.mlsql.indexer.impl._
import tech.mlsql.sqlbooster.meta.ViewCatalyst
import tech.mlsql.tool.{LoadUtils, MLSQLAnalyzer}

/**
 * 2/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class IndexerRewriteController extends CustomController {
  override def run(params: Map[String, String]): String = {

    try {
      ViewCatalyst.createViewCatalyst()

      // 解析load语句
      val mlsqlAnalyzer = new MLSQLAnalyzer(params)

      val stats = mlsqlAnalyzer.analyze
      stats.get.foreach { singleSt => {
        singleSt match {
          case a: LoadStatement =>
            val LoadStatement(_, format, path, option, tableName) = a
            ViewCatalyst.meta.register(tableName, path, format, option)
          case _: SelectStatement =>
          case None => throw new RuntimeException("Only load/select are supported in gen sql interface")

        }
      }
      }

      var temp = ""

      mlsqlAnalyzer.executeAndGetLastTable() match {
        case Some(tableName) =>
          val lp = mlsqlAnalyzer.getSession.table(tableName).queryExecution.analyzed

          val consoleUrl = ScriptSQLExec.context().userDefinedParam.getOrElse("__default__console_url__","")

          val auth_secret = ScriptSQLExec.context().userDefinedParam.getOrElse("__auth_secret__","")

          val metaClient = if (params.getOrElse("isTest", "false").toBoolean) new TestIndexerMeta()
          else new RestIndexerMeta(consoleUrl,auth_secret)

          val indexer = new LinearTryIndexerSelector(Seq(new NestedDataIndexer(metaClient)), metaClient)
          val newPL = indexer.rewrite(lp, Map())
          //          val sparkSession = ScriptSQLExec.context().execListener.sparkSession
          //          val ds = DataSetHelper.create(sparkSession,newPL)
          val loads = ViewCatalyst.meta.values.map(item => LoadUtils.from(item)).mkString("\n")
          temp += loads
          temp += new LogicalPlanSQL(newPL, new MLSQLSQLDialect).toSQL
          temp += "as output;"
        case None =>
      }

      temp
    }
    finally {
      ViewCatalyst.unset
    }

  }
}

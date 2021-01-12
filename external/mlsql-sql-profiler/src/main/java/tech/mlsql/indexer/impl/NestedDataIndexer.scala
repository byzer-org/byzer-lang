package tech.mlsql.indexer.impl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GetJsonObject, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import streaming.dsl.ScriptSQLExec
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.indexer.{IndexerQueryReWriterContext, MLSQLIndexer, MLSQLIndexerMeta}
import tech.mlsql.sqlbooster.meta.ViewCatalyst
import tech.mlsql.tool.LPUtils

/**
 * 17/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class NestedDataIndexer(metaClient: MLSQLIndexerMeta) extends MLSQLIndexer with DslTool {
  //  private val sql = params("sql")
  //  private val jobInfo = JSONTool.parseJson[MLSQLJobInfo](params("__jobinfo__"))

  private val context = ScriptSQLExec.context()
  private val session = context.execListener.sparkSession

  override def rewrite(lp: LogicalPlan, options: Map[String, String]): LogicalPlan = {
    //    val mlsqlAnalyzer = new MLSQLAnalyzer(options ++ Map("sql" -> sql))
    //    val lastTable = mlsqlAnalyzer.executeAndGetLastTable().get
    //    val lp = session.table(lastTable).queryExecution.analyzed
    val joFields = LPUtils.getJsonObjectFields(lp)
    if (joFields.isEmpty) return lp
    val fieldIds = joFields.map(_.exprId).toSet
    val tableWithColumns = LPUtils.getTableAndColumns(lp)

    val tablesMatched = tableWithColumns.filter { case (alias, output) =>
      output.filter(temp => fieldIds.contains(temp.exprId)).size > 0
    }
    if (tablesMatched.isEmpty) return lp


    /**
     * load hive.`wow.abc` as abc;
     *
     *
     * MlsqlOriTable(wow.abc,hive,"") is the table name
     * abc is viewName
     */

    val realTableNames = tablesMatched.map {
      case (alias, _) =>
        ViewCatalyst.meta.getTableNameByViewName(alias)
    }

    val tableToIndexMapping = metaClient.fetchIndexers(realTableNames.toList, Map())
    val indexerQueryReWriterContext = IndexerQueryReWriterContext(session, lp, tableToIndexMapping)
    var newLP = indexerQueryReWriterContext.rewriteWithIndexer

    // replace GetJsonObject
    var isFail = false
    newLP = newLP.transformAllExpressions {
      case a@GetJsonObject(field@AttributeReference(_, _, _, _), path) =>
        val v = path.asInstanceOf[Literal].value.toString
        val newField = v.replaceAll("\\.", "_").replaceFirst("\\$", field.name)
        indexerQueryReWriterContext.getARByName(newField).headOption match {
          case Some(item) =>
            item
          case None =>
            isFail = true
            a
        }
    }
    if (isFail) return lp

    newLP = newLP.transformAllExpressions {
      case field@AttributeReference(_, _, _, _) =>
        if (fieldIds.contains(field.exprId)) {
          isFail = true
        }
        field
    }
    if (isFail) return lp
    // for generating sql
    indexerQueryReWriterContext.fixViewCatalyst
    return newLP
  }

  override def read(sql: LogicalPlan, options: Map[String, String]): Option[DataFrame] = {
    None

  }

  override def write(df: DataFrame, options: Map[String, String]): Option[DataFrame] = ???


  //  def rewrite: LogicalPlan = {
  //
  //  }
}

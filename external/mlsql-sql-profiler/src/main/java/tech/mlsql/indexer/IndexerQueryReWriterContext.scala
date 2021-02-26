package tech.mlsql.indexer

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.job.RunScriptExecutor
import tech.mlsql.sqlbooster.meta.ViewCatalyst
import tech.mlsql.tool.LPUtils

import scala.collection.mutable

case class IndexerQueryReWriterContext(session: SparkSession,
                                       lp: LogicalPlan,
                                       tableToIndexMapping: Map[MlsqlOriTable, List[MlsqlIndexerItem]]
                                      ) {

  private def uuid = UUID.randomUUID().toString.replaceAll("-", "")

  private val _indexerLRDDMapping = new mutable.HashMap[String, LogicalRDD]()
  private val _indexerLRelationMapping = new mutable.HashMap[String, LogicalRelation]()
  private val _viewLoadMapping = new mutable.HashMap[String, (String, String)]()
  private val _arMapping = getARMapping


  def rewriteWithIndexer: LogicalPlan = {

    var temp = lp.transformUp {
      case a@SubqueryAlias(name, r@LogicalRDD(_, _, _, _, _)) =>
        SubqueryAlias(name, _indexerLRDDMapping.getOrElse(name.identifier, r))
      case a@SubqueryAlias(name, r@LogicalRelation(_, _, _, _)) =>
        SubqueryAlias(name, _indexerLRelationMapping.getOrElse(name.identifier, r))
    }
    temp = temp.transformAllExpressions {
      case ar@AttributeReference(_, _, _, _) =>
        val qualifier = ar.qualifier
        _arMapping.getOrElse(ar.withQualifier(Seq()), ar).withQualifier(qualifier)
    }
    temp
  }

  def getARByName(name: String) = {
    _arMapping.values.filter(_.name == name).toList
  }

  def fixViewCatalyst = {
    _viewLoadMapping.foreach {
      case (viewName, (format, path)) =>
        ViewCatalyst.meta.register(viewName, path, format,Map())
    }

  }

  private def getARMapping = {
    val tableWitchColumns = LPUtils.getTableAndColumns(lp)
    val arMapping = new mutable.HashMap[AttributeReference, AttributeReference]()
    tableToIndexMapping.foreach { case (oriTable, indexer) => {
      val nameToArMapping = tableWitchColumns(oriTable.name).map(item => (item.name, item)).toMap
      val (_, indexerAttributes) = getIndexerColumns(oriTable.name, indexer.head)
      indexerAttributes.foreach { item => {
        arMapping += (nameToArMapping.getOrElse(item.name, item) -> item)
      }
      }

    }
    }
    arMapping.toMap
  }

  private def getIndexerColumns(tempViewName: String, indexer: MlsqlIndexerItem) = {

    val params = if (ScriptSQLExec.context() != null) {
      JSONTool.parseJson[Map[String, String]](ScriptSQLExec.context().userDefinedParam("__PARAMS__"))
    } else {
      Map("owner" -> "__system__")
    }

    val tableName = uuid
    val sql =
      s"""
         |load ${indexer.format}.`${indexer.path}`  as ${tableName};
         |""".stripMargin
    val executor = new RunScriptExecutor(params ++ Map("sql" -> sql))
    val tempT = executor.autoClean(false).simpleExecute().get.queryExecution.analyzed
    _viewLoadMapping.put(tempViewName, (indexer.format, indexer.path))
    tempT.transformUp {
      case a@SubqueryAlias(name, r@LogicalRDD(_, _, _, _, _)) =>
        _indexerLRDDMapping.put(tempViewName, r)
        a
      case a@SubqueryAlias(name, r@LogicalRelation(_, _, _, _)) =>
        _indexerLRelationMapping.put(tempViewName, r)
        a
    }

    val tableWitchColumns = LPUtils.getTableAndColumns(tempT)

    tableWitchColumns.toList.head

  }
}

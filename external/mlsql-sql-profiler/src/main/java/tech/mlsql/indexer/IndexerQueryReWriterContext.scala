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
import tech.mlsql.tool.LPUtils

import scala.collection.mutable

case class IndexerQueryReWriterContext(session: SparkSession,
                                       lp: LogicalPlan,
                                       tableToIndexMapping: Map[MlsqlOriTable, MlsqlIndexer]
                                      ) {

  private lazy val _arMapping = getARMapping
  private lazy val _uuid = UUID.randomUUID().toString.replaceAll("-", "")
  private lazy val _indexerLRDDMapping = new mutable.HashMap[String, LogicalRDD]()
  private lazy val _indexerLRelationMapping = new mutable.HashMap[String, LogicalRelation]()

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

  private def getARMapping = {
    val tableWitchColumns = LPUtils.getTableAndColumns(lp)
    val arMapping = new mutable.HashMap[AttributeReference, AttributeReference]()
    tableToIndexMapping.foreach { case (oriTable, indexer) => {
      val nameToArMapping = tableWitchColumns(oriTable.name).map(item => (item.name, item)).toMap
      val (_, indexerAttributes) = getIndexerColumns(oriTable.name, indexer)
      indexerAttributes.foreach { item => {
        arMapping += (nameToArMapping(item.name) -> item)
      }
      }

    }
    }
    arMapping.toMap
  }

  private def getIndexerColumns(tempViewName: String, indexer: MlsqlIndexer) = {

    val params = if (ScriptSQLExec.context() != null) {
      JSONTool.parseJson[Map[String, String]](ScriptSQLExec.context().userDefinedParam("__PARAMS__"))
    } else {
      Map("owner" -> "__system__")
    }

    val tableName = indexer.path + "_" + _uuid
    val sql =
      s"""
         |load ${indexer.format}.`_mlsql_indexer_.${indexer.oriFormat}_${indexer.path.replace(".", "_")}`  as ${tableName};
         |""".stripMargin
    val executor = new RunScriptExecutor(params ++ Map("sql" -> sql))
    val tempT = executor.simpleExecute().get.queryExecution.analyzed

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

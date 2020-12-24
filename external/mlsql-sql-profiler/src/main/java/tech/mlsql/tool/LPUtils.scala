package tech.mlsql.tool

import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GetJsonObject, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import tech.mlsql.indexer.MlsqlIndexer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 17/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object LPUtils {
  def getJsonObjectFields(lp: LogicalPlan) = {
    val joFields = new ArrayBuffer[AttributeReference]()

    lp.transformAllExpressions {
      case a@GetJsonObject(field@AttributeReference(_, _, _, _), path) =>
        joFields += field
        a
    }
    joFields

  }
  
  
  def getTableAndColumns(lp: LogicalPlan) = {
    val tableWithColumns = new mutable.HashMap[String, Seq[AttributeReference]]()
    lp.transformUp {
      case a@SubqueryAlias(name, r@LogicalRDD(_, _, _, _, _)) =>
        tableWithColumns += (name.identifier -> r.output.map(item => item.asInstanceOf[AttributeReference]))
        a
      case a@SubqueryAlias(name, r@LogicalRelation(_, _, _, _)) =>
        tableWithColumns += (name.identifier -> r.output.map(item => item.asInstanceOf[AttributeReference]))
        a
    }
    tableWithColumns
  }
  
  def fixAllRefs(replacedARMapping:mutable.HashMap[AttributeReference, AttributeReference],lp: LogicalPlan) = {
    lp.transformAllExpressions {
      case ar@AttributeReference(_, _, _, _) =>
        val qualifier = ar.qualifier
        replacedARMapping.getOrElse(ar.withQualifier(Seq()), ar).withQualifier(qualifier)
    }
  }
}

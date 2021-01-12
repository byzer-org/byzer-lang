package tech.mlsql.tool

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualNullSafe, EqualTo, Expression, GetJsonObject, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructType

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

  def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  protected def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  def hashCode(_ar: Expression): Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    _ar match {
      case ar@AttributeReference(_, _, _, _) =>
        var h = 17
        h = h * 37 + ar.name.hashCode()
        h = h * 37 + ar.dataType.hashCode()
        h = h * 37 + ar.nullable.hashCode()
        h = h * 37 + ar.metadata.hashCode()
        h = h * 37 + ar.exprId.hashCode()
        h
      case _ => _ar.hashCode()
    }

  }

  /**
   * Rewrite [[EqualTo]] and [[EqualNullSafe]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   */
  private def rewriteEqual(condition: Expression): Expression = condition match {
    case eq@EqualTo(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(hashCode).reduce(EqualTo)
    case eq@EqualNullSafe(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(hashCode).reduce(EqualNullSafe)
    case _ => condition // Don't reorder.
  }

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   * ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   *   etc., will all now be equivalent.*
   * we use new hash function to avoid `ar.qualifier` from alias affect the final order.
   *
   */
  def normalizePlan(plan: LogicalPlan): LogicalPlan = {

    plan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        Filter(splitConjunctivePredicates(condition).map(rewriteEqual).sortBy(hashCode)
          .reduce(And), child)
    }
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

  def getTableAndSchema(lp: LogicalPlan) = {
    val tableWithColumns = new mutable.HashMap[String, StructType]()
    lp.transformUp {
      case a@SubqueryAlias(name, r@LogicalRelation(_, _, _, _)) =>
        tableWithColumns += (name.identifier -> a.schema)
        a
    }
    tableWithColumns
  }

  def fixAllRefs(replacedARMapping: mutable.HashMap[AttributeReference, AttributeReference], lp: LogicalPlan) = {
    lp.transformAllExpressions {
      case ar@AttributeReference(_, _, _, _) =>
        val qualifier = ar.qualifier
        replacedARMapping.getOrElse(ar.withQualifier(Seq()), ar).withQualifier(qualifier)
    }
  }
}

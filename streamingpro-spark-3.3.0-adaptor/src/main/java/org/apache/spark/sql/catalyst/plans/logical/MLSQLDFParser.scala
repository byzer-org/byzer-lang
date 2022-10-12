package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression, UnaryExpression}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 2019-05-19 WilliamZhu(allwefantasy@gmail.com)
 */
object MLSQLDFParser {


  private def collectField(buffer: ArrayBuffer[AttributeReference], o: Expression): Unit = {
    o match {
      case expression: UnaryExpression =>
        collectField(buffer, expression.child)
      case reference: AttributeReference =>
        buffer += reference
      case _ =>
        o.children.foreach { item =>
          collectField(buffer, item)
        }
    }
  }

  def extractTableWithColumns(df: DataFrame): mutable.Map[String, mutable.HashSet[String]] = {
    val tableAndCols = mutable.HashMap.empty[String, mutable.HashSet[String]]
    val relationMap = new mutable.HashMap[Long, String]()

    val analyzed = df.queryExecution.analyzed

    //只对hive表处理，临时表不需要授权
    analyzed.collectLeaves().foreach {
      case r: HiveTableRelation =>
        r.dataCols.foreach(c =>
          relationMap.put(c.exprId.id
            , r.tableMeta.identifier.toString().replaceAll("`", ""))
        )
      case r: LogicalRelation =>
        r.attributeMap.foreach(c =>
          if (r.catalogTable.nonEmpty) {
            relationMap.put(c._2.exprId.id
              , r.catalogTable.get.identifier.toString().replaceAll("`", ""))
          }
        )
      case _ =>
    }

    if (relationMap.nonEmpty) {

      val neSet = mutable.HashSet.empty[NamedExpression]

      analyzed.map {
        case wowLp: Project =>
          wowLp.projectList.map { item =>
            item.collectLeaves().foreach {
              case ne: NamedExpression => neSet.add(ne)
              case _ =>
            }
          }
        case wowLp: Aggregate =>
          wowLp.aggregateExpressions.map { item =>
            item.collectLeaves().foreach {
              case ne: NamedExpression => neSet.add(ne)
              case _ =>
            }
          }
        case _ =>
      }

      val neMap = neSet.map(ne => (ne.exprId.id, ne)).toMap

      relationMap.foreach { x =>
        if (neMap.contains(x._1)) {
          val dbTable = x._2
          val value = tableAndCols.getOrElse(dbTable, mutable.HashSet.empty[String])
          value.add(neMap(x._1).name)
          tableAndCols.update(dbTable, value)
        }
      }
    }

    tableAndCols
  }
}

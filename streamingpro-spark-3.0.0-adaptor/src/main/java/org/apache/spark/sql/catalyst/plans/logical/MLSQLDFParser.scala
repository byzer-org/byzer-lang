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
    if (o.isInstanceOf[UnaryExpression]) {
      collectField(buffer, o.asInstanceOf[UnaryExpression].child)
    } else if (o.isInstanceOf[AttributeReference]) {
      buffer += o.asInstanceOf[AttributeReference]
    }
    else {
      o.children.foreach { item =>
        collectField(buffer, item)
      }
    }
  }

  def extractTableWithColumns(df: DataFrame) = {
    val tableAndCols = mutable.HashMap.empty[String, mutable.HashSet[String]]
    val relationMap = new mutable.HashMap[Long, String]()

    val analyzed = df.queryExecution.analyzed

    //只对hive表处理，临时表不需要授权
    analyzed.collectLeaves().foreach { lp =>
      lp match {
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
    }

    if (relationMap.nonEmpty) {

      val neSet = mutable.HashSet.empty[NamedExpression]

      analyzed.map { lp =>
        lp match {
          case wowLp: Project =>
            wowLp.projectList.map { item =>
              item.collectLeaves().foreach(
                _ match {
                  case ne: NamedExpression => neSet.add(ne)
                  case _ =>
                }
              )
            }
          case wowLp: Aggregate =>
            wowLp.aggregateExpressions.map { item =>
              item.collectLeaves().foreach(
                _ match {
                  case ne: NamedExpression => neSet.add(ne)
                  case _ =>
                }
              )
            }
          case _ =>
        }
      }

      val neMap = neSet.zipWithIndex.map(ne => (ne._1.exprId.id ,ne._1)).toMap

      relationMap.foreach { x =>
        if (neMap.contains(x._1)) {
          val dbTable = x._2
          val value = tableAndCols.getOrElse(dbTable, mutable.HashSet.empty[String])
          value.add(neMap.get(x._1).get.name)
          tableAndCols.update(dbTable, value)
        }
      }
    }

    tableAndCols
  }
}

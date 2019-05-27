package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, UnaryExpression}

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
    var r = Array.empty[String]
    df.queryExecution.logical.map {
      case sp: UnresolvedRelation =>
        r +:= sp.tableIdentifier.unquotedString
      case _ =>
    }
    val tableAndCols = mutable.HashMap.empty[String, mutable.HashSet[String]]
    val mapping = new mutable.HashMap[String, String]()

    def addColumn(o: Attribute) = {
      var qualifier = o.qualifier.mkString(".")

      var counter = 10
      while (mapping.contains(qualifier) && counter > 0) {
        qualifier = mapping(qualifier)
        counter += 1
      }

      if (r.contains(qualifier)) {
        val value = tableAndCols.getOrElse(qualifier, mutable.HashSet.empty[String])
        value.add(o.name)
        tableAndCols.update(qualifier, value)
      }
    }

    // TODO: we should combine these two steps into one step
    // first we collect all alias names
    df.queryExecution.analyzed.map(lp => {
      lp match {

        case wowlp: SubqueryAlias if wowlp.child.isInstanceOf[SubqueryAlias] && r.contains(wowlp.child.asInstanceOf[SubqueryAlias].alias) => {
          val tableName = wowlp.alias
          mapping += (tableName -> wowlp.child.asInstanceOf[SubqueryAlias].alias)
        }
        case _ =>
      }

    })
    // collect all project names
    df.queryExecution.analyzed.map(lp => {
      lp match {
        case wowLp: Project =>
          wowLp.projectList.map { item =>
            val buffer = new ArrayBuffer[AttributeReference]()
            collectField(buffer, item)
            buffer.foreach { ar =>
              addColumn(ar)
            }
          }
        case _ =>
      }

    })

    tableAndCols
  }
}

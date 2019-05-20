package tech.mlsql.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias

import scala.collection.mutable

/**
  * 2019-05-19 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLDFParser {
  def extractTableWithColumns(df: DataFrame) = {
    var r = Array.empty[String]
    df.queryExecution.logical.map {
      case sp: UnresolvedRelation =>
        r +:= sp.tableIdentifier.unquotedString.toLowerCase
      case _ =>
    }
    val tableAndCols = mutable.HashMap.empty[String, mutable.HashSet[String]]
    df.queryExecution.analyzed.map(lp => {
      lp.output.map(o => {
        val qualifier = o.qualifier.mkString(".")
        if (r.contains(qualifier) && !lp.isInstanceOf[SubqueryAlias]) {
          val value = tableAndCols.getOrElse(qualifier, mutable.HashSet.empty[String])
          value.add(o.name)
          tableAndCols.update(qualifier, value)
        }
      })
    })

    tableAndCols
  }
}

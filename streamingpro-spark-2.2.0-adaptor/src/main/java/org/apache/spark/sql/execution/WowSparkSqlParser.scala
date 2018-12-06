package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}

import scala.collection.mutable.ArrayBuffer

/**
  * Concrete parser for Spark SQL statements.
  */
class WowSparkSqlParser(conf: SQLConf) extends AbstractSqlParser {
  val astBuilder = new WowSparkSqlAstBuilder(conf)

  private val substitutor = new VariableSubstitution(conf)

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }

  def tables(sqlText: String, t: ArrayBuffer[TableIdentifier]) = {
    TableHolder.tables.set(t)
    val res = parse(sqlText) { parser =>
      astBuilder.visitSingleStatement(parser.singleStatement()) match {
        case plan: LogicalPlan => plan
        case _ =>
          val position = Origin(None, None)
          throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
      }
    }
    TableHolder.tables.remove()
    res
  }

}

/**
  * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
  */
class WowSparkSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) {
  override def visitTableIdentifier(ctx: TableIdentifierContext): TableIdentifier = {
    val ti = super.visitTableIdentifier(ctx)
    TableHolder.tables.get() += ti
    ti
  }
}

object TableHolder {
  val tables: ThreadLocal[ArrayBuffer[TableIdentifier]] = new ThreadLocal[ArrayBuffer[TableIdentifier]]
}

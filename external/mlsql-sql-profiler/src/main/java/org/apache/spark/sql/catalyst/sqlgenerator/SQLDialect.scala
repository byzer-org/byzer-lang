package org.apache.spark.sql.catalyst.sqlgenerator

/**
 * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
 */

import java.sql.Connection

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait SQLDialect {

  import LogicalPlanSQL._
  import SQLDialect._

  registerDialect(this)

  def relation(alias: String, relation: LogicalRelation): String

  def relation2(alias: String, relation: LogicalRDD): String

  def enableCanonicalize: Boolean

  def canHandle(url: String): Boolean

  def explainSQL(sql: String): String

  def quote(name: String): String

  def maybeQuote(name: String): String

  def getIndexes(conn: Connection, url: String, tableName: String): Set[String]

  def getTableStat(conn: Connection, url: String, tableName: String): ((Option[BigInt], Option[Long]))

  def projectToSQL(p: Project, isDistinct: Boolean, child: String, expression: String): String = {
    build(
      "SELECT",
      if (isDistinct) "DISTINCT" else "",
      expression,
      if (p.child == OneRowRelation) "" else "FROM",
      child)
  }

  def subqueryAliasToSQL(alias: String, child: String) = {
    build(s"($child) $alias")
  }

  def dataTypeToSQL(dataType: DataType): String = {
    dataType.sql
  }

  def literalToSQL(value: Any, dataType: DataType): String = (value, dataType) match {
    case (_, NullType | _: ArrayType | _: MapType | _: StructType) if value == null => "NULL"
    case (v: UTF8String, StringType) => "'" + v.toString.replace("\\", "\\\\").replace("'", "\\'") + "'"
    case (v: Byte, ByteType) => v + ""
    case (v: Boolean, BooleanType) => s"'$v'"
    case (v: Short, ShortType) => v + ""
    case (v: Long, LongType) => v + ""
    case (v: Float, FloatType) => v + ""
    case (v: Double, DoubleType) => v + ""
    case (v: Decimal, t: DecimalType) => v + ""
    case (v: Int, DateType) => s"'${DateTimeUtils.toJavaDate(v)}'"
    case (v: Long, TimestampType) => s"'${DateTimeUtils.toJavaTimestamp(v)}'"
    case _ => if (value == null) "NULL" else value.toString
  }

  def limitSQL(sql: String, limit: String): String = {
    s"$sql LIMIT $limit"
  }

  def joinSQL(p: Join, left: String, right: String, condition: String): String = {
    build(
      left,
      p.joinType.sql,
      "JOIN",
      right,
      condition)
  }

  def getAttributeName(e: AttributeReference): String = {
    val qualifierPrefix = e.qualifier.map(_ + ".").headOption.getOrElse("")
    s"$qualifierPrefix${quote(e.name)}"
  }

  def expressionToSQL(e: Expression): String = {
    e.prettyName
  }

}

object SQLDialect {
  private[this] var dialects = List[SQLDialect]()


  def registerDialect(dialect: SQLDialect): Unit = synchronized {
    dialects = dialect :: dialects.filterNot(_ == dialect)
  }

  def unregisterDialect(dialect: SQLDialect): Unit = synchronized {
    dialects = dialects.filterNot(_ == dialect)
  }

  def get(url: String): SQLDialect = {
    val matchingDialects = dialects.filter(_.canHandle(url))
    matchingDialects.headOption match {
      case None => throw new NoSuchElementException(s"no suitable MbDialect from $url")
      case Some(d) => d
    }
  }

}

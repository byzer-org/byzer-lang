/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
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

  private val substitutor = new VariableSubstitution

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }

  def tables(sqlText: String, t: ArrayBuffer[WowTableIdentifier]) = {
    TableHolder.tables.set(t)
    val res = try {
      parse(sqlText) { parser =>
        astBuilder.visitSingleStatement(parser.singleStatement()) match {
          case plan: LogicalPlan => plan
          case _ =>
            val position = Origin(None, None)
            throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
        }
      }
    } finally {
      TableHolder.tables.remove()
    }
    res
  }

}

/**
  * Builder that converts an ANTLR ParseTree into a LogicalPlan/Expression/TableIdentifier.
  */
class WowSparkSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder {
  override def visitTableIdentifier(ctx: TableIdentifierContext): TableIdentifier = {
    val ti = super.visitTableIdentifier(ctx)
    TableHolder.tables.get() += WowTableIdentifier(ti.table, ti.database, None)
    ti
  }

  override def visitCreateTableHeader(ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    val tableHeader = super.visitCreateTableHeader(ctx)

    TableHolder.tablesSet(tableHeader._1, None)
    tableHeader
  }

  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] = {
    val multipartIdentifier = super.visitMultipartIdentifier(ctx)

    val ifInsert = ctx.parent.getChild(0).getText.toLowerCase match {
      case "insert" => Some("insert")
      case _ => None
    }

    TableHolder.tablesSet(multipartIdentifier, ifInsert)
    multipartIdentifier
  }

}

object TableHolder {
  val tables: ThreadLocal[ArrayBuffer[WowTableIdentifier]] = new ThreadLocal[ArrayBuffer[WowTableIdentifier]]

  def tablesSet(multipartIdentifier: Seq[String], oprationType: Option[String]) = multipartIdentifier.size match {
    case 2 => TableHolder.tables.get() += WowTableIdentifier(multipartIdentifier(1), Option(multipartIdentifier.head), oprationType)
    case 1 => TableHolder.tables.get() += WowTableIdentifier(multipartIdentifier.head, None, oprationType)
    case _ =>
  }

}

case class WowTableIdentifier(table: String, database: Option[String], operator: Option[String]) {
  val identifier: String = table

  def this(table: String) = this(table, None, None)
}
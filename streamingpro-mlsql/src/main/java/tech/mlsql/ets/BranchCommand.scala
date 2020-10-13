package tech.mlsql.ets

import java.util.UUID

import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.dsl.parser.DSLSQLLexer
import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.dsl.{IfContext, ScriptSQLExec}
import tech.mlsql.dsl.adaptor.DslAdaptor
import tech.mlsql.lang.cmd.compile.internal.gc._

import scala.collection.mutable

/**
 * 5/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait BranchCommand {

  def str(ctx: SqlContext) = {

    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    input.getText(interval)
  }

  def indent = {
    val n = ScriptSQLExec.context().execListener.branchContext.contexts.size
    " " * n
  }

  def pushTrace(s: String): Unit = {
    val newstr = indent + s
    ScriptSQLExec.context().execListener.branchContext.traces.append(newstr)
  }

  def getTraces = {
    ScriptSQLExec.context().execListener.branchContext.traces
  }

  def traceBC = {
    ScriptSQLExec.context().execListener.env().getOrElse("__debug__", "false").toBoolean
  }

  def branchContext = {
    ScriptSQLExec.context().execListener.branchContext.contexts
  }

  def emptyDF = {
    ScriptSQLExec.context().execListener.sparkSession.emptyDataFrame
  }

  def session = {
    ScriptSQLExec.context().execListener.sparkSession
  }

  def ifContextInit: BranchCommand = {
    val uuid = UUID.randomUUID().toString.replaceAll("-", "")
    val variables = new mutable.HashMap[String, Any]
    val types = new mutable.HashMap[String, Any]

    val v = branchContext.headOption match {
      case Some(_c) =>
        val c = _c.asInstanceOf[IfContext]
        variables ++= c.variableTable.variables
        types ++= c.variableTable.types
        VariableTable(uuid, variables, types)
      case None =>

        variables ++= ScriptSQLExec.context().execListener.env()
        VariableTable(uuid, variables, types)
    }
    ScriptSQLExec.context().execListener.branchContext.contexts.push(IfContext(
      new mutable.ArrayBuffer[DslAdaptor](),
      new mutable.ArrayBuffer[SqlContext](),
      v,
      false,
      false,
      false
    ))
    this
  }

  private def rowToMap(df: DataFrame) = {
    val row = df.collect().head.toSeq
    val fields = df.schema.map(_.name).toSeq
    val schema = df.schema.map(_.dataType).toSeq
    (fields.zip(row).toMap, fields.zip(schema).toMap)
  }

  def evaluateIfElse(ifContext: IfContext, str: String, options: Map[String, String] = Map()): (Boolean, IfContext) = {
    val session = ScriptSQLExec.context().execListener.sparkSession
    val scanner = new Scanner(str)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val exprs = try {
      parser.parse()
    } catch {
      case e: ParserException =>
        throw new MLSQLException(s"Error in MLSQL Line:${options.getOrElse("__LINE__", "-1").toInt + 1} \n Expression:${e.getMessage}")
      case e: Exception => throw e

    }
    val sQLGenContext = new SQLGenContext(session)
    

    val item = sQLGenContext.execute(exprs.map(_.asInstanceOf[Expression]), ifContext.variableTable)
    val lit = item.asInstanceOf[Literal]

    val (_variables, _types) = rowToMap(session.table(ifContext.variableTable.name))
    val variables = new mutable.HashMap[String, Any]
    val types = new mutable.HashMap[String, Any]
    variables ++= _variables
    types ++= _types

    val newifContext = ifContext.copy(variableTable = VariableTable(ifContext.variableTable.name, variables, types))
    //clean temp table
    session.catalog.dropTempView(newifContext.variableTable.name)
    lit.dataType match {
      case Types.Boolean => (lit.value.toString.toBoolean, newifContext)
    }
  }
}

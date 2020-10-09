package tech.mlsql.ets

import org.antlr.v4.runtime.misc.Interval
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

  def str(ctx:SqlContext)  = {

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

  def pushTrace(s:String):Unit = {
      val newstr =  indent + s
      ScriptSQLExec.context().execListener.branchContext.traces.append(newstr)
  }

  def getTraces = {
    ScriptSQLExec.context().execListener.branchContext.traces
  }

  def traceBC = {
    ScriptSQLExec.context().execListener.env().getOrElse("__debug__","false").toBoolean
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
    ScriptSQLExec.context().execListener.branchContext.contexts.push(IfContext(
      new mutable.ArrayBuffer[DslAdaptor](),
      new mutable.ArrayBuffer[SqlContext](),
      false,
      false,
      false
    ))
    this
  }

  def evaluate(str: String) = {
    val baseInput = ScriptSQLExec.context().execListener.env()
    val session = ScriptSQLExec.context().execListener.sparkSession
    val scanner = new Scanner(str)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val exprs = parser.parse()
    val sQLGenContext = new SQLGenContext(session)
    val item = sQLGenContext.execute(exprs.map(_.asInstanceOf[Expression]), baseInput.toMap)
    val lit = item.asInstanceOf[Literal]
    lit.dataType match {
      case Types.Boolean => lit.value.toString.toBoolean
    }
  }
}

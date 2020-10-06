package tech.mlsql.ets

import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.dsl.{IfContext, ScriptSQLExec}
import tech.mlsql.dsl.adaptor.DslAdaptor
import tech.mlsql.lang.cmd.compile.internal.gc.{Expression, Scanner, StatementParser, Tokenizer}

import scala.collection.mutable

/**
 * 5/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait BranchCommand {

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
    item.asInstanceOf[Boolean]
  }
}

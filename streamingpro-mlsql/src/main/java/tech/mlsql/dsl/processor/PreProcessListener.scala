package tech.mlsql.dsl.processor

import streaming.dsl.ScriptSQLExecListener
import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.parser.lisener.BaseParseListenerextends
import tech.mlsql.dsl.adaptor.{CommandAdaptor, SetAdaptor, SingleStatement, StatementAdaptor}
import tech.mlsql.{MLSQLEnvKey, Stage}

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
  */
class PreProcessListener(val scriptSQLExecListener: ScriptSQLExecListener) extends BaseParseListenerextends {

  private val _statements = new ArrayBuffer[String]()
  private val _singleStatements = new ArrayBuffer[SingleStatement]()

  def toScript = {
    scriptSQLExecListener.addEnv(MLSQLEnvKey.CONTEXT_STATEMENT_NUM, _statements.length.toString)
    _statements.mkString(";") + ";"
  }

  def statements = {
    _statements
  }

  def analyzedStatements = {
    _singleStatements
  }

  def addStatement(v: String) = {
    _statements += v
    this
  }

  def addSingleStatement(v: SingleStatement) = {
    _singleStatements += v
    this
  }

  override def exitSql(ctx: SqlContext): Unit = {

    ctx.getChild(0).getText.toLowerCase() match {
      case item if item.startsWith("!") =>
        new CommandAdaptor(this).parse(ctx)
        new StatementAdaptor(this, (raw) => {}).parse(ctx)
      case "set" => {
        new SetAdaptor(scriptSQLExecListener, Stage.preProcess).parse(ctx)
        new StatementAdaptor(this, (raw) => {
          addStatement(raw)
        }).parse(ctx)
      }
      case _ => new StatementAdaptor(this, (raw) => {
        addStatement(raw)
      }).parse(ctx)
    }

  }


}

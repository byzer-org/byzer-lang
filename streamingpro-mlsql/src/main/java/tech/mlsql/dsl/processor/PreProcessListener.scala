package tech.mlsql.dsl.processor

import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.dsl.{ScriptSQLExecListener, SetAdaptor}
import streaming.parser.lisener.BaseParseListenerextends
import tech.mlsql.dsl.adaptor.{CommandAdaptor, StatementAdaptor}
import tech.mlsql.{MLSQLEnvKey, Stage}

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
  */
class PreProcessListener(val scriptSQLExecListener: ScriptSQLExecListener) extends BaseParseListenerextends {

  private val _statements = new ArrayBuffer[String]()

  def toScript = {
    scriptSQLExecListener.addEnv(MLSQLEnvKey.CONTEXT_STATEMENT_NUM, _statements.length.toString)
    _statements.mkString(";") + ";"
  }

  def statements = {
    _statements
  }

  def addStatement(v: String) = {
    _statements += v
    this
  }

  override def exitSql(ctx: SqlContext): Unit = {

    ctx.getChild(0).getText.toLowerCase() match {
      case item if item.startsWith("!") =>
        new CommandAdaptor(this).parse(ctx)
      case "set" => {
        new SetAdaptor(scriptSQLExecListener, Stage.preProcess).parse(ctx)
        new StatementAdaptor(this).parse(ctx)
      }
      case _ => new StatementAdaptor(this).parse(ctx)
    }

  }


}

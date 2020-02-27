package tech.mlsql.dsl.adaptor

import streaming.dsl.ScriptSQLExecListener
import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.parser.lisener.BaseParseListenerextends

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
  */
class PreProcessIncludeListener(val scriptSQLExecListener: ScriptSQLExecListener) extends BaseParseListenerextends {

  private val _statements = new ArrayBuffer[StatementChunk]()

  def toScript = {
    val stringBuffer = new StringBuffer()
    _statements.foreach { f =>
      f.st match {
        case SCType.Normal =>
          stringBuffer.append(f.content + ";")
        case SCType.Include =>
          stringBuffer.append(f.content)
      }
    }
    stringBuffer.toString
  }

  def addStatement(v: String, scType: SCType.Value) = {
    _statements += StatementChunk(v, scType)
    this
  }

  override def exitSql(ctx: SqlContext): Unit = {

    ctx.getChild(0).getText.toLowerCase() match {
      case "include" =>
        new IncludeAdaptor(this).parse(ctx)
      case _ => new StatementForIncludeAdaptor(this).parse(ctx)
    }

  }

}

case class StatementChunk(content: String, st: SCType.Value)

object SCType extends Enumeration {
  val Include, Normal = Value
}


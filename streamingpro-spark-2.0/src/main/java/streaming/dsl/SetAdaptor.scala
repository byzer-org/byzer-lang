package streaming.dsl

import _root_.streaming.dsl.parser.DSLSQLParser._
import streaming.common.ShellCommand

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SetAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var key = ""
    var value = ""
    var command = ""
    var option = Map[String, String]()
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: SetKeyContext =>
          key = s.getText
        case s: SetValueContext =>
          if (s.quotedIdentifier() != null && s.quotedIdentifier().BACKQUOTED_IDENTIFIER() != null) {
            command = cleanStr(s.getText)
          }
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> cleanStr(s.STRING().getText))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> cleanStr(s.expression().STRING().getText))
        case _ =>
      }
    }

    option.get("type") match {
      case Some("sql") =>
        val resultHead = scriptSQLExecListener.sparkSession.sql(command).collect().headOption
        if (resultHead.isDefined) {
          value = resultHead.get.get(0).toString
        }
      case Some("shell") =>
        value = ShellCommand.execSimpleCommand(command).trim
      case _ =>
        value = ShellCommand.execSimpleCommand(command).trim
    }

    scriptSQLExecListener.addEnv(key, value)
    //scriptSQLExecListener.sparkSession.sql(ctx.)
  }
}

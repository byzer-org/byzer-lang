package streaming.dsl

import _root_.streaming.dsl.parser.DSLSQLParser._
import streaming.common.ShellCommand
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SetAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var key = ""
    var value = ""
    var command = ""
    var original_command = ""
    var option = Map[String, String]()
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: SetKeyContext =>
          key = s.getText
        case s: SetValueContext =>
          original_command = s.getText
          if (s.quotedIdentifier() != null && s.quotedIdentifier().BACKQUOTED_IDENTIFIER() != null) {
            command = cleanStr(s.getText)
          } else if (s.qualifiedName() != null && s.qualifiedName().identifier() != null) {
            command = cleanStr(s.getText)
          }
          else {
            command = original_command
          }
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> cleanStr(s.STRING().getText))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> cleanStr(s.expression().STRING().getText))
        case _ =>
      }
    }

    def evaluate(str: String) = {
      TemplateMerge.merge(str, scriptSQLExecListener.env().toMap)
    }

    var overwrite = true
    option.get("type") match {
      case Some("sql") =>

        val resultHead = scriptSQLExecListener.sparkSession.sql(evaluate(command)).collect().headOption
        if (resultHead.isDefined) {
          value = resultHead.get.get(0).toString
        }
      case Some("shell") =>
        value = ShellCommand.execSimpleCommand(evaluate(command)).trim
      case Some("conf") =>
        scriptSQLExecListener.sparkSession.sql(s""" set ${key} = ${original_command} """)
      case Some("defaultParam") =>
        overwrite = false
      case _ =>
        value = cleanBlockStr(cleanStr(command))
    }

    if (!overwrite) {
      if (!scriptSQLExecListener.env().contains(key)) {
        scriptSQLExecListener.addEnv(key, value)
      }
    } else {
      scriptSQLExecListener.addEnv(key, value)
    }

    scriptSQLExecListener.env().view.foreach {
      case (k, v) =>
        val mergedValue = TemplateMerge.merge(v, scriptSQLExecListener.env().toMap)
        if (mergedValue != v) {
          scriptSQLExecListener.addEnv(k, mergedValue)
        }
    }

    scriptSQLExecListener.setLastSelectTable(null)
    //scriptSQLExecListener.sparkSession.sql(ctx.)
  }
}

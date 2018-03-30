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
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: SetKeyContext =>
          key = s.getText
        case s: SetValueContext =>
          value = cleanStr(s.getText)
          if (s.quotedIdentifier() != null && s.quotedIdentifier().BACKQUOTED_IDENTIFIER() != null) {
            value = ShellCommand.execSimpleCommand(value).trim
          }

        case _ =>
      }
    }
    scriptSQLExecListener.addEnv(key, value)
  }
}

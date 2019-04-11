package tech.mlsql.dsl

import streaming.dsl.ScriptSQLExecListener

/**
  * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
  */
object CommandCollection {
  def fill(context: ScriptSQLExecListener): Unit = {
    context.addEnv("desc", """run command as ShowTableExt.`{}`""")
  }
}

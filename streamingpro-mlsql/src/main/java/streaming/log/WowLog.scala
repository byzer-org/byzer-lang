package streaming.log

import streaming.dsl.ScriptSQLExec

/**
  * Created by allwefantasy on 4/9/2018.
  */
trait WowLog {
  def format(msg: String) = {
    s"""[owner] [${ScriptSQLExec.context().owner}] $msg"""
  }
}

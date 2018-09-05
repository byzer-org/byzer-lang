package streaming.log

import streaming.dsl.ScriptSQLExec

/**
  * Created by allwefantasy on 4/9/2018.
  */
trait WowLog {
  def format(msg: String) = {
    s"""[owner] [${ScriptSQLExec.context().owner}] $msg"""
  }

  def format_exception(e: Exception) = {
    (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => format(f)).toSeq.mkString("\n")
  }
}

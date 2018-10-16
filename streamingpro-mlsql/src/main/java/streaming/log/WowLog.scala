package streaming.log

import streaming.dsl.ScriptSQLExec

/**
  * Created by allwefantasy on 4/9/2018.
  */
trait WowLog {
  def format(msg: String) = {
    if (ScriptSQLExec.context() != null) {
      s"""[owner] [${ScriptSQLExec.context().owner}] $msg"""
    } else {
      s"""[owner] [null] $msg"""
    }

  }

  def format_exception(e: Exception) = {
    (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => format(f)).toSeq.mkString("\n")
  }

  def format_throwable(e: Throwable) = {
    (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => format(f)).toSeq.mkString("\n")
  }

  def format_cause(e: Exception) = {
    var cause = e.asInstanceOf[Throwable]
    while (cause.getCause != null) {
      cause = cause.getCause
    }
    format_throwable(cause)
  }
}

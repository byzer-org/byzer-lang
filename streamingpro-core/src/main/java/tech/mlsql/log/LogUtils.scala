package tech.mlsql.log

import streaming.dsl.ScriptSQLExec

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-08-21 WilliamZhu(allwefantasy@gmail.com)
  */
object LogUtils {
  def format(msg: String, skipPrefix: Boolean = false) = {
    if (skipPrefix) {
      msg
    } else {
      if (ScriptSQLExec.context() != null) {
        val context = ScriptSQLExec.context()
        s"""[owner] [${context.owner}] [groupId] [${context.groupId}] __MMMMMM__ $msg"""
      } else {
        s"""[owner] [null] [groupId] [null] __MMMMMM__ $msg"""
      }
    }


  }

  def formatWithOwner(msg: String, owner: String, groupId: String) = {
    s"""[owner] [${owner}] [groupId] [${groupId}] __MMMMMM__ $msg"""
  }

  def wow_format(msg: String) = {
    format(msg)

  }

  def format_exception(e: Exception) = {
    (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => format(f)).toSeq.mkString("\n")
  }

  def format_throwable(e: Throwable, skipPrefix: Boolean = false) = {
    (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => format(f, skipPrefix)).toSeq.mkString("\n")
  }

  def format_cause(e: Exception) = {
    var cause = e.asInstanceOf[Throwable]
    while (cause.getCause != null) {
      cause = cause.getCause
    }
    format_throwable(cause)
  }

  def format_full_exception(buffer: ArrayBuffer[String], e: Exception, skipPrefix: Boolean = true) = {
    var cause = e.asInstanceOf[Throwable]
    buffer += format_throwable(cause, skipPrefix)
    while (cause.getCause != null) {
      cause = cause.getCause
      buffer += "caused by：\n" + format_throwable(cause, skipPrefix)
    }

  }
}

package tech.mlsql.runtime.plugins.exception_render

import streaming.log.WowLog
import tech.mlsql.app.ExceptionRender

import scala.collection.mutable.ArrayBuffer


class DefaultExceptionRender extends ExceptionRender with WowLog {
  override def format(e: Exception): String = {
    e.printStackTrace()
    val msgBuffer = ArrayBuffer[String]()
    format_full_exception(msgBuffer, e)
    e.getMessage + "\n" + msgBuffer.mkString("\n")
  }

  override def is_match(e: Exception): Boolean = true
}

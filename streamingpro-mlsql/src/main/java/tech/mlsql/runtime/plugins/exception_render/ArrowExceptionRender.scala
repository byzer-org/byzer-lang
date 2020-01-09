package tech.mlsql.runtime.plugins.exception_render

import streaming.log.WowLog
import tech.mlsql.app.ExceptionRender

import scala.collection.mutable.ArrayBuffer

/**
 * 9/1/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ArrowExceptionRender extends ExceptionRender with WowLog {
  override def format(e: Exception): String = {
    val buffer = ArrayBuffer[String]()

    val header =
      s"""
         |##################################################
         |#                                                #
         |# This exception is may caused by [Schema Error].#
         |# The original exception is:                     #
         |#                                                #
         |##################################################
         |""".stripMargin

    buffer += header

    format_full_exception(buffer, e, true)
    buffer.mkString("\n")
  }

  override def is_match(e: Exception): Boolean = {
    recognizeError(e)
  }

  private def recognizeError(e: Exception) = {
    val buffer = ArrayBuffer[String]()
    format_full_exception(buffer, e, true)
    val typeError = buffer.filter(f => f.contains("Previous exception in task: null")).filter(_.contains("org.apache.spark.sql.vectorized.ArrowColumnVector$ArrowVectorAccessor")).size > 0
    typeError
  }
}

package tech.mlsql.app

import org.apache.spark.sql.DataFrame
import tech.mlsql.common.utils.log.Logging


trait App {
  def run(args: Seq[String]): Unit
}

trait CustomController {
  def run(params: Map[String, String]): String
}

trait RequestCleaner extends Logging {
  def run(): Unit

  final def call() = {
    try {
      run()
    } catch {
      case e: Exception => log.error("RequestCleaner Error: {}", e)
    }

  }
}

trait ExceptionRender extends Logging {
  def format(e: Exception): String

  def is_match(e: Exception): Boolean

  final def call(e: Exception): ExceptionResult = {
    try {
      if (is_match(e)) {
        ExceptionResult(e, Option(format(e)))
      } else {
        ExceptionResult(e, None)
      }

    } catch {
      case e1: Exception =>
        log.debug("ExceptionRender Error: {}", e1)
        ExceptionResult(e, None)
    }

  }
}

case class ResultResp(df: DataFrame,name:String)

trait ResultRender {
  def call(d: ResultResp): ResultResp
}

case class ExceptionResult(e: Exception, str: Option[String])


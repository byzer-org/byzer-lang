package tech.mlsql.app


trait App {
  def run(args: Seq[String]): Unit
}

trait CustomController {
  def run(params: Map[String, String]): String
}

trait RequestCleaner {
  def run(): Unit

  final def call() = {
    try {
      run()
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }
}

trait ExceptionRender {
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
      case e1: Exception => e1.printStackTrace()
        ExceptionResult(e, None)
    }

  }
}

case class ExceptionResult(e: Exception, str: Option[String])


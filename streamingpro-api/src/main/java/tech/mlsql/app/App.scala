package tech.mlsql.app


trait App {
  def run(args: Seq[String]): Unit
}

trait CustomController {
  def run(params: Map[String, String]): String
}


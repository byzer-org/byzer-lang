package tech.mlsql.dsl.auth

import streaming.dsl.auth.MLSQLTable

/**
  * 2019-05-19 WilliamZhu(allwefantasy@gmail.com)
  */
trait DatasourceAuth {
  def auth(path: String, params: Map[String, String]): List[MLSQLTable]
}

object AuthMode extends Enumeration {
  type mode = Value
  val compile = Value("compile")
  val runtime = Value("runtime")

  def keyName = "mode"

}

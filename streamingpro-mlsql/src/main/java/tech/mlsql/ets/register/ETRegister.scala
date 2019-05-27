package tech.mlsql.ets.register

import tech.mlsql.ets.MLSQLEventCommand

/**
  * 2019-04-12 WilliamZhu(allwefantasy@gmail.com)
  */
object ETRegister {
  private def wow(name: String) = name -> ("tech.mlsql.ets." + name)

  val mapping = Map[String, String](
    wow("ShowCommand"),
    wow("EngineResource"),
    wow("HDFSCommand"),
    wow("NothingET"),
    wow("ModelCommand"),
    wow("MLSQLEventCommand")
  )

}

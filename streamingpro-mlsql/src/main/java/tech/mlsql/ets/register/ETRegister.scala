package tech.mlsql.ets.register


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
    wow("MLSQLEventCommand"),
    wow("KafkaCommand"),
    wow("DeltaCompactionCommand"),
    wow("DeltaCompactionCommandWrapper"),
    wow("ShowTablesExt")
  )

}

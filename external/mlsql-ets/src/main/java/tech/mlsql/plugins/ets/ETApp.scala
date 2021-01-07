package tech.mlsql.plugins.ets

import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 6/8/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ETApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("Pivot", classOf[Pivot].getName)
    //CommandCollection.refreshCommandMapping(Map(ProfilerApp.COMMAND_NAME -> ProfilerApp.MODULE_NAME))
    ETRegister.register("SaveBinaryAsFile", classOf[SaveBinaryAsFile].getName)
    CommandCollection.refreshCommandMapping(Map("saveFile" ->
      """
        |run ${i} as SaveBinaryAsFile.`` where filePath="${o}"
        |""".stripMargin))

    ETRegister.register("TableRepartition", classOf[TableRepartition].getName)
    CommandCollection.refreshCommandMapping(Map("tableRepartition" ->
      """
        |run ${i} as TableRepartition.`` where partitionNum="${num}" as ${o}
        |""".stripMargin))

    ETRegister.register("LastTableName", classOf[LastTableName].getName)
    CommandCollection.refreshCommandMapping(Map("lastTableName" -> "LastTableName"))

    ETRegister.register("LastCommand", classOf[LastCommand].getName)
    CommandCollection.refreshCommandMapping(Map("lastCommand" -> "LastCommand"))


    ETRegister.register("EmptyTable", classOf[EmptyTable].getName)
    CommandCollection.refreshCommandMapping(Map("emptyTable" -> "EmptyTable"))

    ETRegister.register("SchemaCommand", classOf[SchemaCommand].getName)
    ETRegister.register("StreamJobs", classOf[StreamJobs].getName)
    //    CommandCollection.refreshCommandMapping(Map("emptyTable" -> "SchemaCommand"))
  }


  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}


object ETApp {

}

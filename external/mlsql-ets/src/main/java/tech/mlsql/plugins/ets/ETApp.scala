package tech.mlsql.plugins.ets

import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 6/8/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ETApp  extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    ETRegister.register("Pivot", classOf[Pivot].getName)
    //CommandCollection.refreshCommandMapping(Map(ProfilerApp.COMMAND_NAME -> ProfilerApp.MODULE_NAME))
    ETRegister.register("SaveBinaryAsFile", classOf[SaveBinaryAsFile].getName)
    CommandCollection.refreshCommandMapping(Map("saveFile" ->
      """
        |run ${i} as SaveBinaryAsFile.`` where filePath="${o}"
        |""".stripMargin))
  }


  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}


object ETApp {
  
}

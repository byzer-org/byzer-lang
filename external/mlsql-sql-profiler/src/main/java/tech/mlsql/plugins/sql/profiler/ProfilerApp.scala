package tech.mlsql.plugins.sql.profiler

import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.indexer.{CubeIndexerBuilder, CubeIndexerQuery}
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.version.VersionCompatibility

/**
 * 27/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ProfilerApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AppRuntimeStore.store.registerController("genSQL", classOf[GenSQLController].getName)
    AppRuntimeStore.store.registerController("indexRewrite", classOf[IndexerRewriteController].getName)
    ETRegister.register(ProfilerApp.MODULE_NAME, classOf[ProfilerCommand].getName)
    ETRegister.register("ZOrdering", classOf[ZOrdering].getName)
    ETRegister.register("CubeIndexerBuilder", classOf[CubeIndexerBuilder].getName)
    ETRegister.register("CubeIndexerQuery", classOf[CubeIndexerQuery].getName)
    CommandCollection.refreshCommandMapping(Map(ProfilerApp.COMMAND_NAME -> ProfilerApp.MODULE_NAME))
    AppRuntimeStore.store.registerResultRender("IndexerPlugin", classOf[IndexerPlugin].getName)
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
}

object ProfilerApp {
  val MODULE_NAME = "ProfilerCommand"
  val COMMAND_NAME = "profiler"
}
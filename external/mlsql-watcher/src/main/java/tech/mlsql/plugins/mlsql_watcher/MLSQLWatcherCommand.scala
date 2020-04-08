package tech.mlsql.plugins.mlsql_watcher

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams
import tech.mlsql.common.utils.base.Measurement
import tech.mlsql.common.utils.distribute.socket.server.JavaUtils
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.plugins.mlsql_watcher.db.PluginDB.ctx
import tech.mlsql.plugins.mlsql_watcher.db.PluginDB.ctx._
import tech.mlsql.plugins.mlsql_watcher.db.{CustomDB, CustomDBWrapper, PluginDB, WKv}
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.store.{DBStore, DictType}

/**
 * 11/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLWatcherCommand(override val uid: String) extends SQLAlg with WowParams {
  def this() = this(WowParams.randomUID())

  // !watcher db add dbName dbConfig
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val args = JSONTool.parseJson[List[String]](params("parameters"))
    args match {
      case List("db", "add", dbName, dbConfig) =>
        AppRuntimeStore.store.store.write(CustomDBWrapper(CustomDB(PluginDB.plugin_name, dbName, dbConfig)))
        //persist
        DBStore.store.saveConfig(df.sparkSession, PluginDB.plugin_name, dbName, dbConfig, DictType.DB)
        DBStore.store.saveConfig(df.sparkSession, PluginDB.plugin_name, PluginDB.plugin_name, dbName, DictType.APP_TO_DB)
      case List("cleaner", time) =>
        val seconds = Measurement.timeStringAsSec(time)
        SnapshotTimer.cleanerThresh.set(seconds * 1000)
        if (MLSQLWatcherCommand.isDbConfigured) {
          ctx.run(query[WKv].insert(_.name -> lift(MLSQLWatcherCommand.KEY_CLEAN_TIME), _.value -> lift((seconds * 1000).toString)).
            onConflictUpdate((t, e) => t.value -> e.value))
        }

    }
    df.sparkSession.emptyDataFrame
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

}

object MLSQLWatcherCommand {
  def isDbConfigured = {
    AppRuntimeStore.store.store.count(classOf[CustomDBWrapper]) > 0
  }

  val KEY_CLEAN_TIME = "cleanTime"
}

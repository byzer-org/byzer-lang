package tech.mlsql.runtime

import java.io.File

import com.fasterxml.jackson.annotation.JsonIgnore
import net.csdn.jpa.QuillDB
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.runtime.kvstore.KVIndex
import tech.mlsql.runtime.metastore.MySQLDBStore
import tech.mlsql.store.DBStore

/**
 * 16/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MetaStoreService extends MLSQLRuntimeLifecycle {
  override def beforeRuntimeStarted(params: Map[String, String], conf: SparkConf): Unit = {}

  override def afterRuntimeStarted(params: Map[String, String], conf: SparkConf, rootSparkSession: SparkSession): Unit = {
    params.get(MetaStoreService.KEY) match {
      case Some("mysql") =>
        val name = params(MetaStoreService.DB_NAME_KEY)
        val config = params(MetaStoreService.DB_CONFIG_KEY)
        val configStr = scala.io.Source.fromFile(new File(config)).getLines().mkString("\n")
        AppRuntimeStore.store.store.write(WCustomDBWrapper(WCustomDB(classOf[MetaStoreService].getName, name, configStr)))
        DBStore.set(new MySQLDBStore)
      case None =>
    }
  }
}

object MetaStoreService extends Logging {
  val KEY = "streaming.metastore.db.type"
  val DB_NAME_KEY = "streaming.metastore.db.name"
  val DB_CONFIG_KEY = "streaming.metastore.db.config.path"

  val KEY_VALUE = "mysql"

  lazy val ctx = {
    val dbWrapper = AppRuntimeStore.store.store.read(classOf[WCustomDBWrapper], classOf[MetaStoreService].getName)
    val dbInfo = dbWrapper.db
    logInfo(dbInfo.dbName)
    logInfo(dbInfo.dbConfig)
    QuillDB.createNewCtxByNameFromStr(dbInfo.dbName, dbInfo.dbConfig)
  }
}

case class WCustomDBWrapper(db: WCustomDB) {
  @JsonIgnore
  @KVIndex
  def id = db.name
}

case class WCustomDB(name: String, dbName: String, dbConfig: String)

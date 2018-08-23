package org.apache.spark

import java.util.HashMap
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import scala.collection.JavaConverters._

/**
  * Created by allwefantasy on 3/6/2018.
  */
object MLSQLConf {
  private[this] val mlsqlConfEntries = new HashMap[String, ConfigEntry[_]]()

  def register(entry: ConfigEntry[_]): Unit = {
    require(!mlsqlConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    mlsqlConfEntries.put(entry.key, entry)
  }

  def entries = mlsqlConfEntries

  private[this] object MLSQLConfigBuilder {
    def apply(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)
  }

  val SESSION_IDLE_TIMEOUT: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.mlsql.session.idle.timeout")
      .doc("SparkSession timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(30L))

  val SESSION_CHECK_INTERVAL: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.mlsql.session.check.interval")
      .doc("The check interval for backend session a.k.a SparkSession timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(5L))

  val BIND_HOST: ConfigEntry[String] =
    MLSQLConfigBuilder("spark.mlsql.bind.host")
      .doc("Bind host on which to run.")
      .stringConf
      .createWithDefault(MLSQLSparkConst.localHostName())

  val SESSION_WAIT_OTHER_TIMES: ConfigEntry[Int] =
    MLSQLConfigBuilder("spark.mlsql.session.wait.other.times")
      .doc("How many times to check when another session with the same user is initializing " +
        "SparkContext. Total Time will be times by " +
        "`spark.mlsql.session.wait.other.interval`")
      .intConf
      .createWithDefault(60)

  val SESSION_WAIT_OTHER_INTERVAL: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.mlsql.session.wait.other.interval")
      .doc("The interval for checking whether other thread with the same user has completed" +
        " SparkContext instantiation.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(1L))

  val SESSTION_INIT_TIMEOUT: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.mlsql.session.init.timeout")
      .doc("How long we suggest the server to give up instantiating SparkContext")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(TimeUnit.SECONDS.toSeconds(60L))

  def getAllDefaults: Map[String, String] = {
    entries.entrySet().asScala.map {kv =>
      (kv.getKey, kv.getValue.defaultValueString)
    }.toMap
  }
}

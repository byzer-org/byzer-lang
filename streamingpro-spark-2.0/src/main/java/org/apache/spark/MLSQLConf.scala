package org.apache.spark

import java.util.HashMap
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

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

  private[this] object MLSQLConfigBuilder {
    def apply(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)
  }

  val BACKEND_SESSION_IDLE_TIMEOUT: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.mlsql.backend.session.idle.timeout")
      .doc("SparkSession timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(30L))

  val BACKEND_SESSION_CHECK_INTERVAL: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.mlsql.backend.session.check.interval")
      .doc("The check interval for backend session a.k.a SparkSession timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.MINUTES.toMillis(5L))

  val FRONTEND_BIND_HOST: ConfigEntry[String] =
    MLSQLConfigBuilder("spark.kyuubi.frontend.bind.host")
      .doc("Bind host on which to run the Kyuubi Server.")
      .stringConf
      .createWithDefault(MLSQLSparkConst.localHostName())

  val BACKEND_SESSION_WAIT_OTHER_TIMES: ConfigEntry[Int] =
    MLSQLConfigBuilder("spark.kyuubi.backend.session.wait.other.times")
      .doc("How many times to check when another session with the same user is initializing " +
        "SparkContext. Total Time will be times by " +
        "`spark.kyuubi.backend.session.wait.other.interval`")
      .intConf
      .createWithDefault(60)

  val BACKEND_SESSION_WAIT_OTHER_INTERVAL: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.kyuubi.backend.session.wait.other.interval")
      .doc("The interval for checking whether other thread with the same user has completed" +
        " SparkContext instantiation.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(TimeUnit.SECONDS.toMillis(1L))

  val BACKEND_SESSTION_INIT_TIMEOUT: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.kyuubi.backend.session.init.timeout")
      .doc("How long we suggest the server to give up instantiating SparkContext")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(TimeUnit.SECONDS.toSeconds(60L))


}

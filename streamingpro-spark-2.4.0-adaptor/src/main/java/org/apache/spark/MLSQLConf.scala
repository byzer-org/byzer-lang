/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.util.concurrent.TimeUnit
import java.util.{HashMap, Map => JMap}

import org.apache.spark.internal.config._

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

  val MLSQL_PLATFORM = MLSQLConfigBuilder("streaming.platform")
    .doc("for now only supports spark,spark_streaming,flink").stringConf.checkValues(Set("spark", "spark_streaming", "flink")).createOptional

  val MLSQL_ENABLE_REST = MLSQLConfigBuilder("streaming.rest").doc(
    """
      |enable rest api
    """.stripMargin).booleanConf.createWithDefault(false)

  val MLSQL_DRIVER_PORT = MLSQLConfigBuilder("streaming.driver.port").doc(
    """
      | The port of rest api
    """.stripMargin).intConf.createWithDefault(9003)


  val MLSQL_MASTER = MLSQLConfigBuilder("streaming.master")
    .doc("the same with spark master").stringConf.createOptional

  val MLSQL_NAME: ConfigEntry[String] = MLSQLConfigBuilder("streaming.name")
    .doc("The name will showed in yarn cluster and spark ui").stringConf.createWithDefault("mlsql")

  val MLSQL_BIGDL_ENABLE: ConfigEntry[Boolean] = MLSQLConfigBuilder("streaming.bigdl.enable")
    .doc(
      """
        |Enable embedded deep learning support.
        |if set true, then we will automatically add spark conf like following:
        |
        |  conf.setIfMissing("spark.shuffle.reduceLocality.enabled", "false")
        |  conf.setIfMissing("spark.shuffle.blockTransferService", "nio")
        |  conf.setIfMissing("spark.scheduler.minRegisteredResourcesRatio", "1.0")
        |  conf.setIfMissing("spark.speculation", "false")
      """.stripMargin).booleanConf.createWithDefault(true)

  val MLSQL_CLUSTER_PS_ENABLE: ConfigEntry[Boolean] = MLSQLConfigBuilder("streaming.ps.cluster.enable").doc(
    """
      |MLSQL supports directly communicating with executor if you set this true.
    """.stripMargin).booleanConf.createWithDefault(true)

  val MLSQL_CLUSTER_PS_DRIVER_PORT: ConfigEntry[Int] = MLSQLConfigBuilder("spark.ps.cluster.driver.port").doc(
    """
      |ps driver port
    """.stripMargin).intConf.createWithDefault(7777)

  val MLSQL_PS_ASK_TIMEOUT: ConfigEntry[Long] = MLSQLConfigBuilder("streaming.ps.ask.timeout").doc(
    s"""
       |control how long distributing resource/python env take then timeout. unit: seconds
     """.stripMargin).longConf.createWithDefault(3600)

  val MLSQL_PS_NETWORK_TIMEOUT: ConfigEntry[Long] = MLSQLConfigBuilder("streaming.ps.network.timeout").doc(
    s"""
       |set spark.network.timeout
     """.stripMargin).longConf.createWithDefault(60 * 60 * 8)

  val MLSQL_HIVE_CONNECTION = MLSQLConfigBuilder("streaming.hive.javax.jdo.option.ConnectionURL").doc(
    """
      |Use this to configure `hive.javax.jdo.option.ConnectionURL`
    """.stripMargin).stringConf.createWithDefault("")

  val MLSQL_ENABLE_HIVE_SUPPORT: ConfigEntry[Boolean] = MLSQLConfigBuilder("streaming.enableHiveSupport").doc(
    """
      |enable hive
    """.stripMargin).booleanConf.createWithDefault(false)

  val MLSQL_ENABLE_CARBONDATA_SUPPORT: ConfigEntry[Boolean] = MLSQLConfigBuilder("streaming.enableCarbonDataSupport").doc(
    """
      |carbondata support. If set true, please configure `streaming.carbondata.store` and `streaming.carbondata.meta`
    """.stripMargin).booleanConf.createWithDefault(false)


  val MLSQL_CARBONDATA_STORE = MLSQLConfigBuilder("streaming.carbondata.store").doc(
    """
      |
    """.stripMargin).stringConf.createOptional

  val MLSQL_CARBONDATA_META = MLSQLConfigBuilder("streaming.carbondata.meta").doc(
    """
      |
    """.stripMargin).stringConf.createOptional

  val MLSQL_DEPLOY_REST_API = MLSQLConfigBuilder("streaming.deploy.rest.api").doc(
    """
      |If you deploy MLSQL as predict service, please enable this.
    """.stripMargin).booleanConf.createWithDefault(false)

  val MLSQL_SPARK_SERVICE = MLSQLConfigBuilder("streaming.spark.service").doc(
    """
      |Run MLSQL as service and without quit.
    """.stripMargin).booleanConf.createWithDefault(false)

  
  val MLSQL_DISABLE_SPARK_LOG = MLSQLConfigBuilder("streaming.disableSparkLog").doc(
    """
      |Sometimes there are too much spark job info, you can disable them.
    """.stripMargin).booleanConf.createWithDefault(false)

  val MLSQL_UDF_CLZZNAMES = MLSQLConfigBuilder("streaming.udf.clzznames").doc(
    """
      |register udf class
    """.stripMargin).stringConf.createOptional

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

  val ENABLE_MAX_RESULT_SIZE: ConfigEntry[Boolean] =
    MLSQLConfigBuilder("spark.mlsql.enable.max.result.limit")
      .doc("enable restful max result size limitation. when you enable this configuration." +
        " you should pass `maxResultSize` for your rest request." +
        " if not, you take only max 1000 record.")
      .booleanConf
      .createWithDefault(false)

  val RESTFUL_API_MAX_RESULT_SIZE: ConfigEntry[Long] =
    MLSQLConfigBuilder("spark.mlsql.restful.api.max.result.size")
      .doc("the max size of restful api result.")
      .longConf
      .createWithDefault(1000)

  val MLSQL_LOG: ConfigEntry[Boolean] = MLSQLConfigBuilder("streaming.executor.log.in.driver")
    .doc("Executor send log msg to driver.")
    .booleanConf
    .createWithDefault(true)


  def getAllDefaults: Map[String, String] = {
    entries.entrySet().asScala.map { kv =>
      (kv.getKey, kv.getValue.defaultValueString)
    }.toMap
  }

  def createConfigReader(settings: JMap[String, String]) = {
    val reader = new ConfigReader(new MLSQLConfigProvider(settings))
    reader
  }
}

private[spark] class MLSQLConfigProvider(conf: JMap[String, String]) extends ConfigProvider {

  override def get(key: String): Option[String] = {
    if (key.startsWith("streaming.")) {
      Option(conf.get(key)).orElse(SparkConf.getDeprecatedConfig(key, conf))
    } else {
      None
    }
  }

}

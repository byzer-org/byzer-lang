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

package streaming.core.strategy.platform

import java.lang.reflect.Modifier
import java.util.concurrent.atomic.AtomicReference
import java.util.{UUID, Map => JMap}

import _root_.streaming.core.stream.MLSQLStreamManager
import net.csdn.common.reflect.ReflectHelper
import org.apache.spark.ps.cluster.PSDriverBackend
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.mlsql.session.{SessionIdentifier, SessionManager}
import org.apache.spark.sql.{MLSQLUtils, SQLContext, SparkSession}
import org.apache.spark.{MLSQLConf, SparkConf, SparkRuntimeOperator, WowFastSparkContext}
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.lang.sc.ScalaObjectReflect
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.datalake.DataLake
import tech.mlsql.job.JobManager
import tech.mlsql.log.DriverLogServer
import tech.mlsql.runtime.{AsSchedulerService, MLSQLRuntimeLifecycle}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by allwefantasy on 30/3/2017.
 */
class SparkRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener with Logging {

  // init
  val configReader = MLSQLConf.createConfigReader(params.map(f => (f._1.toString, f._2.toString)))

  def name = "SPARK"

  registerJdbcDialect(HiveJdbcDialect)

  val lifeCyleCallback = ArrayBuffer[MLSQLRuntimeLifecycle](
    new AsSchedulerService()
  )
  var psDriverBackend: PSDriverBackend = null

  var driverLogServer: DriverLogServer[String] = null

  var sparkSession: SparkSession = createRuntime

  var sessionManager = new SessionManager(sparkSession)
  sessionManager.start()

  postInit()

  //---------------------FUNCTIONS---------------------------------
  def getSession(owner: String) = {
    sessionManager.getSession(SessionIdentifier(owner)).sparkSession
  }

  def getMLSQLSession(owner: String) = {
    sessionManager.getSession(SessionIdentifier(owner))
  }

  def operator = {
    new SparkRuntimeOperator(sparkSession)
  }

  def createRuntime = {
    logInfo("create Runtime...")

    val conf = new SparkConf()
    params.filter(f =>
      f._1.toString.startsWith("spark.") ||
        f._1.toString.startsWith("hive.")
    ).foreach { f =>
      conf.set(f._1.toString, f._2.toString)
    }
    if (MLSQLConf.MLSQL_MASTER.readFrom(configReader).isDefined) {
      conf.setMaster(MLSQLConf.MLSQL_MASTER.readFrom(configReader).get)
    }

    conf.setAppName(MLSQLConf.MLSQL_NAME.readFrom(configReader))

    if (MLSQLConf.MLSQL_BIGDL_ENABLE.readFrom(configReader)) {
      conf.setIfMissing("spark.shuffle.reduceLocality.enabled", "false")
      conf.setIfMissing("spark.shuffle.blockTransferService", "nio")
      conf.setIfMissing("spark.scheduler.minRegisteredResourcesRatio", "1.0")
      conf.setIfMissing("spark.speculation", "false")
    }

    if (MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.readFrom(configReader)) {
      logInfo("PSExecutor configured...")
      conf.set("spark.network.timeout", MLSQLConf.MLSQL_PS_NETWORK_TIMEOUT.readFrom(configReader) + "s")
      conf.set("spark.executor.plugins", "org.apache.spark.ps.cluster.PSExecutorPlugin")


      val port = NetUtils.availableAndReturn(MLSQLUtils.localCanonicalHostName, 7778, 7999)

      if (port == -1) {
        throw new RuntimeException(s"Fail to create for ps cluster, maybe executor cannot bind port ")
      }
      conf.set(MLSQLConf.MLSQL_CLUSTER_PS_DRIVER_PORT.key, port.toString)
    }

    /**
     * start a log server ,so the executor can send log to driver and the driver will log them into log files.
     */
    val confSparkService = MLSQLConf.MLSQL_SPARK_SERVICE.readFrom(configReader)
    //    val confLogInDriver = MLSQLConf.MLSQL_LOG.readFrom(configReader)
    val confLogInDriver = params.asScala.getOrElse("streaming.executor.log.in.driver", "true").toString.toBoolean
    if ((confSparkService && confLogInDriver) ||
      params.getOrDefault("streaming.unittest", "false").toString.toBoolean) {
      val token = UUID.randomUUID().toString
      driverLogServer = new DriverLogServer[String](new AtomicReference[String](token))
      logInfo(s"DriverLogServer is started in ${driverLogServer._host}:${driverLogServer._port.toString} with token:${token}")
      conf.set("spark.mlsql.log.driver.host", driverLogServer._host)
      conf.set("spark.mlsql.log.driver.port", driverLogServer._port.toString)
      conf.set("spark.mlsql.log.driver.token", token)
    }

    if (params.containsKey(DataLake.USER_KEY)) {
      conf.set(DataLake.RUNTIME_KEY, params.get(DataLake.USER_KEY).toString)
    }

    registerLifeCyleCallback("tech.mlsql.runtime.MetaStoreService")

    lifeCyleCallback.foreach { callback =>
      callback.beforeRuntimeStarted(params.map(f => (f._1.toString, f._2.toString)).toMap, conf)
    }

    val sparkSession = SparkSession.builder().config(conf)

    def setHiveConnectionURL = {
      val url = MLSQLConf.MLSQL_HIVE_CONNECTION.readFrom(configReader)
      if (!url.isEmpty) {
        logInfo("set hive javax.jdo.option.ConnectionURL=" + url)
        sparkSession.config("javax.jdo.option.ConnectionURL", url)
      }
    }

    if (MLSQLConf.MLSQL_ENABLE_HIVE_SUPPORT.readFrom(configReader)) {
      setHiveConnectionURL
      sparkSession.enableHiveSupport()
    }

    if (MLSQLConf.MLSQL_DEPLOY_REST_API.readFrom(configReader)) {
      conf.setIfMissing("spark.default.parallelism", "1")
        .setIfMissing("spark.sql.shuffle.partitions", "1")
      val wfsc = new WowFastSparkContext(conf)
      ReflectHelper.method(sparkSession, "sparkContext", wfsc)
    }
    val ss = sparkSession.getOrCreate()

    lifeCyleCallback.foreach { callback =>
      callback.afterRuntimeStarted(params.map(f => (f._1.toString, f._2.toString)).toMap, conf, ss)
    }

    if (MLSQLConf.MLSQL_SPARK_SERVICE.readFrom(configReader)) {
      JobManager.init(ss)
    }

    if (MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.readFrom(configReader)) {
      logInfo("PSDriver starting...")
      psDriverBackend = new PSDriverBackend(ss.sparkContext)
      psDriverBackend.start()
    }

    show(params.asScala.map(kv => (kv._1.toString, kv._2.toString)).toMap)
    ss
  }

  def postInit() = {
    params.put("_session_", sparkSession)
    registerUDF("streaming.core.compositor.spark.udf.Functions")
    registerUDF("tech.mlsql.crawler.udf.Functions")
    registerUDF("tech.mlsql.udf.Functions")
    if (params.containsKey(MLSQLConf.MLSQL_UDF_CLZZNAMES.key)) {
      MLSQLConf.MLSQL_UDF_CLZZNAMES.readFrom(configReader).get.split(",").foreach { clzz =>
        registerUDF(clzz)
      }
    }
    MLSQLStreamManager.start(sparkSession)
    createTables
  }


  def createTables = {
    sparkSession.sql("select 1 as a").createOrReplaceTempView("command")
  }

  def registerLifeCyleCallback(name: String) = {
    try {
      val instance = Class.forName(name).newInstance().asInstanceOf[MLSQLRuntimeLifecycle]
      lifeCyleCallback += instance

    } catch {
      case e: Exception =>
        logInfo(s"Fail to register cycle callback :${name}")
    }
  }

  def registerJdbcDialect(dialect: JdbcDialect) = {
    logInfo("register HiveSqlDialect.....")
    JdbcDialects.registerDialect(dialect)
  }

  def registerUDF(clzz: String) = {
    logInfo("register functions.....")
    ClassLoaderTool.classForName(clzz).getMethods.foreach { f =>
      try {
        if (Modifier.isStatic(f.getModifiers)) {
          f.invoke(null, sparkSession.udf)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

  }

  override def startRuntime: StreamingRuntime = {
    this
  }

  override def awaitTermination: Unit = {
    if (MLSQLConf.MLSQL_SPARK_SERVICE.readFrom(configReader)) {
      Thread.currentThread().join()
    }
  }


  override def streamingRuntimeInfo: StreamingRuntimeInfo = null

  override def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean): Boolean = {
    sparkSession.stop()
    SparkRuntime.clearLastInstantiatedContext()
    true
  }


  override def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo): Unit = {}

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator): Unit = {

  }

  override def params: JMap[Any, Any] = _params

  override def processEvent(event: Event): Unit = {}

  SparkRuntime.setLastInstantiatedContext(this)

  override def startThriftServer: Unit = {
    val (clzz, instance) = ScalaObjectReflect.findObjectMethod("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2")
    val method = clzz.getMethod("startWithContext", classOf[SQLContext])
    method.invoke(instance, sparkSession.sqlContext)
  }

  override def startHttpServer: Unit = {}

  private def show(conf: Map[String, String]) {
    val keyLength = conf.keys.map(_.size).max
    val valueLength = conf.values.map(_.size).max
    val header = "-" * (keyLength + valueLength + 3)
    logInfo("mlsql server start with configuration!")
    logInfo(header)
    conf.map {
      case (key, value) =>
        val keyStr = key + (" " * (keyLength - key.size))
        val valueStr = value + (" " * (valueLength - value.size))
        s"|${keyStr}|${valueStr}|"
    }.foreach(line => {
      logInfo(line)
    })
    logInfo(header)
  }

}

object SparkRuntime {

  private val INSTANTIATION_LOCK = new Object()

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[SparkRuntime]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   */
  def getOrCreate(params: JMap[Any, Any]): SparkRuntime = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new SparkRuntime(params)
      }
    }
    PlatformManager.getOrCreate.register(lastInstantiatedContext.get())
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      PlatformManager.getOrCreate.unRegister(lastInstantiatedContext.get())
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(sparkRuntime: SparkRuntime): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sparkRuntime)
    }
  }
}
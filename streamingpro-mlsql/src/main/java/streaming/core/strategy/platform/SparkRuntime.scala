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
import java.util.{Map => JMap}

import _root_.streaming.common.{NetUtils, ScalaObjectReflect}
import _root_.streaming.core.StreamingproJobManager
import _root_.streaming.dsl.mmlib.algs.bigdl.WowLoggerFilter
import _root_.streaming.log.Logging
import net.csdn.common.reflect.ReflectHelper
import org.apache.spark._
import org.apache.spark.ps.cluster.PSDriverBackend
import org.apache.spark.ps.local.LocalPSSchedulerBackend
import org.apache.spark.sql.mlsql.session.{SessionIdentifier, SessionManager}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by allwefantasy on 30/3/2017.
  */
class SparkRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener with Logging {

  val configReader = MLSQLConf.createConfigReader(params.map(f => (f._1.toString, f._2.toString)))

  def name = "SPARK"

  var localSchedulerBackend: LocalPSSchedulerBackend = null
  var psDriverBackend: PSDriverBackend = null

  var sparkSession: SparkSession = createRuntime

  var sessionManager = new SessionManager(sparkSession)
  sessionManager.start()

  def getSession(owner: String) = {
    sessionManager.getSession(SessionIdentifier(owner)).sparkSession
  }

  def closeSession(owner: String) = {
    sessionManager.closeSession(SessionIdentifier(owner))
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

    def isLocalMaster(conf: SparkConf): Boolean = {
//      val master = MLSQLConf.MLSQL_MASTER.readFrom(configReader).getOrElse("")
      val master = conf.get("spark.master", "")
      master == "local" || master.startsWith("local[")
    }

    if (MLSQLConf.MLSQL_BIGDL_ENABLE.readFrom(configReader)) {
      conf.setIfMissing("spark.shuffle.reduceLocality.enabled", "false")
      conf.setIfMissing("spark.shuffle.blockTransferService", "nio")
      conf.setIfMissing("spark.scheduler.minRegisteredResourcesRatio", "1.0")
      conf.setIfMissing("spark.speculation", "false")
    }

    if (MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.readFrom(configReader) && !isLocalMaster(conf)) {
      logWarning(
        s"""
           |------------------------------------------------------------------------
           |${MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.key} is enabled. Please make sure
           |you have the uber-jar of mlsql placed in
           |1. --jars
           |2. --conf "spark.executor.extraClassPath=[your jar name in jars]"
           |
           |for exmaple:
           |
           |--jars ./streamingpro-mlsql-spark_2.x-x.x.x-SNAPSHOT.jar
           |--conf "spark.executor.extraClassPath=streamingpro-mlsql-spark_2.x-x.x.x-SNAPSHOT.jar"
           |
           |Otherwise the executor will
           |fail to start and the whole application will fails.
           |------------------------------------------------------------------------
          """.stripMargin)

      logInfo("register worker.sink.pservice.class with org.apache.spark.ps.cluster.PSServiceSink")
      conf.set("spark.metrics.conf.executor.sink.pservice.class", "org.apache.spark.ps.cluster.PSServiceSink")
      val holdPort = NetUtils.availableAndReturn(7778, 7999)

      if (holdPort == null) {
        throw new RuntimeException(s"Fail to create for ps cluster, maybe executor cannot bind port ")
      }
      val port = holdPort.getLocalPort
      conf.set(MLSQLConf.MLSQL_CLUSTER_PS_DRIVER_PORT.key, port.toString)
    }


    //    SQLDL4J.tm = SQLDL4J.init(isLocalMaster(conf))

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

    val checkCarbonDataCoreCompatibility = CarbonCoreVersion.coreCompatibility(SparkCoreVersion.version, SparkCoreVersion.exactVersion)
    val isCarbonDataEnabled = MLSQLConf.MLSQL_ENABLE_CARBONDATA_SUPPORT.readFrom(configReader) && checkCarbonDataCoreCompatibility

    if (!checkCarbonDataCoreCompatibility) {
      logWarning(s"------- CarbonData do not support current version of spark [${SparkCoreVersion.exactVersion}], streaming.enableCarbonDataSupport will not take effect.--------")
    }

    val ss = if (isCarbonDataEnabled) {

      logInfo("CarbonData enabled...")
      setHiveConnectionURL
      val carbonBuilder = Class.forName("org.apache.spark.sql.CarbonSession$CarbonBuilder").
        getConstructor(classOf[SparkSession.Builder]).
        newInstance(sparkSession)
      Class.forName("org.apache.spark.sql.CarbonSession$CarbonBuilder").
        getMethod("getOrCreateCarbonSession", classOf[String], classOf[String]).
        invoke(carbonBuilder, params("streaming.carbondata.store").toString, params("streaming.carbondata.meta").toString).asInstanceOf[SparkSession]
    } else {
      if (MLSQLConf.MLSQL_DEPLOY_REST_API.readFrom(configReader)) {
        conf.setIfMissing("spark.default.parallelism", "1")
          .setIfMissing("spark.sql.shuffle.partitions", "1")
        val wfsc = new WowFastSparkContext(conf)
        ReflectHelper.method(sparkSession, "sparkContext", wfsc)
      }
      sparkSession.getOrCreate()
    }


    if (MLSQLConf.MLSQL_SPARK_SERVICE.readFrom(configReader)) {
      StreamingproJobManager.init(ss.sparkContext)
    }

    // parameter server should be enabled by default

    if (MLSQLConf.MLSQL_LOCAL_PS_ENABLE.readFrom(configReader) && isLocalMaster(conf)) {
      logInfo("start LocalPSSchedulerBackend")
      localSchedulerBackend = new LocalPSSchedulerBackend(ss.sparkContext)
      localSchedulerBackend.start()
    }

    if (MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.readFrom(configReader) && !isLocalMaster(conf)) {
      logInfo("start PSDriverBackend")
      psDriverBackend = new PSDriverBackend(ss.sparkContext)
      psDriverBackend.start()
    }

    if (MLSQLConf.MLSQL_DISABLE_SPARK_LOG.readFrom(configReader)) {
      WowLoggerFilter.redirectSparkInfoLogs()
    }
    show(params.asScala.map(kv => (kv._1.toString, kv._2.toString)).toMap)
    ss
  }

  params.put("_session_", sparkSession)

  registerUDF("streaming.core.compositor.spark.udf.Functions")

  if (params.containsKey(MLSQLConf.MLSQL_UDF_CLZZNAMES.key)) {
    MLSQLConf.MLSQL_UDF_CLZZNAMES.readFrom(configReader).get.split(",").foreach { clzz =>
      registerUDF(clzz)
    }
  }


  def registerUDF(clzz: String) = {
    logInfo("register functions.....")
    Class.forName(clzz).getMethods.foreach { f =>
      try {
        if (Modifier.isStatic(f.getModifiers)) {
          logInfo(f.getName)
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

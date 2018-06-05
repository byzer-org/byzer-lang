package streaming.core.strategy.platform


import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger
import java.util.{Map => JMap}

import org.apache.spark.{MLSQLConf, SparkConf, SparkContext, SparkRuntimeOperator}
import org.apache.spark.ps.cluster.PSDriverBackend
import org.apache.spark.ps.local.LocalPSSchedulerBackend
import org.apache.spark.sql.internal.StaticSQLConf
import streaming.session.{SessionIdentifier, SessionManager, SparkSessionCacheManager}

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 4/6/2018.
  */
class MLSQLRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener {
  val logger = Logger.getLogger(getClass.getName)

  def name = "MLSQL"

  var localSchedulerBackend: LocalPSSchedulerBackend = null
  var psDriverBackend: PSDriverBackend = null

  var adminSessionIdentifier: SessionIdentifier = null

  private[this] var sessionManager: SessionManager = _

  def getSessionManager: SessionManager = sessionManager

  private[this] val sparkContext: AtomicReference[SparkContext] = new AtomicReference[SparkContext]()


  def operator = {
    new SparkRuntimeOperator(getSessionManager.getSession(adminSessionIdentifier).sparkSession)
  }

  def createSparkConf = {
    logger.info("create Runtime...")
    val conf = new SparkConf()
    params.filter(f =>
      f._1.toString.startsWith("spark.") ||
        f._1.toString.startsWith("hive.")
    ).foreach { f =>
      conf.set(f._1.toString, f._2.toString)
    }
    if (params.containsKey("streaming.master")) {
      conf.setMaster(params.get("streaming.master").toString)
    }

    conf.setAppName(params.get("streaming.name").toString)

    if (params.containsKey("streaming.ps.enable") && params.get("streaming.ps.enable").toString.toBoolean) {
      if (!isLocalMaster(conf)) {
        logger.info("register worker.sink.pservice.class with org.apache.spark.ps.cluster.PSServiceSink")
        conf.set("spark.metrics.conf.executor.sink.pservice.class", "org.apache.spark.ps.cluster.PSServiceSink")
      }
    }

    if (params.containsKey("streaming.enableHiveSupport") &&
      params.get("streaming.enableHiveSupport").toString.toBoolean) {
      conf.set(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
    }



    MLSQLConf.getAllDefaults.foreach(kv => conf.setIfMissing(kv._1, kv._2))
    conf
  }

  def isLocalMaster(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master == "local" || master.startsWith("local[")
  }

  override def startRuntime: StreamingRuntime = {
    val conf = createSparkConf

    val allowMultipleContexts: Boolean =
      conf.getBoolean("spark.driver.allowMultipleContexts", false)

    if (!allowMultipleContexts) {
      sparkContext.set(new SparkContext(conf))
      params.put("__sc__", sparkContext.get())
    }

    sessionManager = new SessionManager(conf)
    sessionManager.start()
    adminSessionIdentifier = sessionManager.openSession("admin", "", "", sessionManager.conf.getAll.toMap, params.toMap, true)
    val session = sessionManager.getSession(adminSessionIdentifier)

    // parameter server should be enabled by default
    if (!params.containsKey("streaming.ps.enable") || !params.get("streaming.ps.enable").toString.toBoolean) {
      logger.info("ps enabled...")
      if (session.sparkSession.sparkContext.isLocal) {
        localSchedulerBackend = new LocalPSSchedulerBackend(session.sparkSession.sparkContext)
        localSchedulerBackend.start()
      } else {
        logger.info("start PSDriverBackend")
        psDriverBackend = new PSDriverBackend(session.sparkSession.sparkContext)
        psDriverBackend.start()
      }
    }

    this
  }

  override def awaitTermination: Unit = {
    if (params.getOrElse("streaming.spark.service", false).toString.toBoolean) {
      Thread.currentThread().join()
    }
  }


  override def streamingRuntimeInfo: StreamingRuntimeInfo = null

  override def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean): Boolean = {
    SparkSessionCacheManager.get.stop()
    MLSQLRuntime.clearLastInstantiatedContext()
    true
  }


  override def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo): Unit = {}

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator): Unit = {

  }

  override def params: JMap[Any, Any] = _params

  override def processEvent(event: Event): Unit = {}


  override def startThriftServer: Unit = {

  }

  override def startHttpServer: Unit = {}

  MLSQLRuntime.setLastInstantiatedContext(this)
}

object MLSQLRuntime {


  private val INSTANTIATION_LOCK = new Object()

  /**
    * Reference to the last created SQLContext.
    */
  @transient private val lastInstantiatedContext = new AtomicReference[MLSQLRuntime]()

  /**
    * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
    * This function can be used to create a singleton SQLContext object that can be shared across
    * the JVM.
    */
  def getOrCreate(params: JMap[Any, Any]): MLSQLRuntime = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new MLSQLRuntime(params)
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

  private[platform] def setLastInstantiatedContext(mlsqlRuntime: MLSQLRuntime): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(mlsqlRuntime)
    }
  }
}

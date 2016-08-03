package streaming.core.strategy.platform

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.{List => JList, Map => JMap}

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import net.csdn.common.logging.Loggers
import net.csdn.common.network.NetworkUtils.StackType
import net.csdn.common.settings.ImmutableSettings
import net.csdn.common.settings.ImmutableSettings._
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.zk.{ZKClient, ZKConfUtil}
import streaming.common.{ParamsUtil, SQLContextHolder}
import streaming.core.strategy.JobStrategy
import streaming.core.{Dispatcher, StreamingApp}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 4/27/16 WilliamZhu(allwefantasy@gmail.com)
 */


class PlatformManager {
  self =>
  val config = new AtomicReference[ParamsUtil]()

  val logger = Loggers.getLogger(classOf[PlatformManager])

  def findDispatcher(contextParams: JMap[Any, Any]): StrategyDispatcher[Any] = {
    Dispatcher.dispatcher(contextParams)
  }

  def findDispatcher: StrategyDispatcher[Any] = {
    Dispatcher.dispatcher(Dispatcher.contextParams(""))
  }

  val listeners = new ArrayBuffer[PlatformManagerListener]()

  def register(listener: PlatformManagerListener) = {
    listeners += listener
  }

  def unRegister(listener: PlatformManagerListener) = {
    listeners -= listener
  }

  def startRestServer = {
    ServiceFramwork.scanService.setLoader(classOf[StreamingApp])
    ServiceFramwork.enableNoThreadJoin()
    Application.main(Array())
  }

  def registerToZk(params: ParamsUtil) = {
    val settingsB: ImmutableSettings.Builder = settingsBuilder()
    settingsB.put(ServiceFramwork.mode + ".zk.conf_root_dir", params.getParam("streaming.zk.conf_root_dir"))
    settingsB.put(ServiceFramwork.mode + ".zk.servers", params.getParam("streaming.zk.servers"))
    zk = new ZKClient(settingsB.build())
    val client = zk.zkConfUtil.client

    if (!client.exists(ZKConfUtil.CONF_ROOT_DIR)) {
      client.createPersistent(ZKConfUtil.CONF_ROOT_DIR, true);
    }

    if (client.exists(ZKConfUtil.CONF_ROOT_DIR + "/address")) {
      throw new RuntimeException(s"${ZKConfUtil.CONF_ROOT_DIR} already exits in zookeeper")
    }
    val hostAddress = net.csdn.common.network.NetworkUtils.getFirstNonLoopbackAddress(StackType.IPv4).getHostAddress
    val port = params.getParam("streaming.driver.port", "9003")
    logger.info(s"register ip and port to zookeeper:\n" +
      s"zk=[${params.getParam("streaming.zk.servers")}]\n" +
      s"${ZKConfUtil.CONF_ROOT_DIR}/address=${hostAddress}:${port}")

    client.createEphemeral(ZKConfUtil.CONF_ROOT_DIR + "/address", hostAddress + ":" + port)
  }

  var zk: ZKClient = null


  def run(_params: ParamsUtil, reRun: Boolean = false) = {

    if (!reRun) {
      config.set(_params)
    }

    val params = config.get()

    val lastStreamingRuntimeInfo = if (reRun) {
      val tempRuntime = PlatformManager.getRuntime
      SparkStreamingRuntime.clearLastInstantiatedContext()
      Some(tempRuntime.streamingRuntimeInfo)
    } else None


    val tempParams = new java.util.HashMap[Any, Any]()
    params.getParamsMap.filter(f => f._1.startsWith("streaming.")).foreach { f => tempParams.put(f._1, f._2) }
    val runtime = PlatformManager.getRuntime


    val dispatcher = findDispatcher

    var jobs: Array[String] = dispatcher.strategies.filter(f => f._2.isInstanceOf[JobStrategy]).keys.toArray

    if (params.hasParam("streaming.jobs"))
      jobs = params.getParam("streaming.jobs").split(",")



    lastStreamingRuntimeInfo match {
      case Some(ssri) =>
        runtime.configureStreamingRuntimeInfo(ssri)
        runtime.resetRuntimeOperator(null)
      case None =>
    }

    if (params.getBooleanParam("streaming.rest", false) && !reRun) {
      startRestServer
    }
    if (params.hasParam("streaming.zk.conf_root_dir") && !reRun) {
      registerToZk(params)
    }


    val jobCounter = new AtomicInteger(0)
    jobs.foreach {
      jobName =>
        dispatcher.dispatch(Dispatcher.contextParams(jobName))
        val index = jobCounter.get()

        listeners.foreach { listener =>
          listener.processEvent(JobFlowGenerate(jobName, index, dispatcher.findStrategies(jobName).get.head))
        }
        jobCounter.incrementAndGet()
    }

    runtime.startRuntime

    runtime.awaitTermination
  }

  PlatformManager.setLastInstantiatedContext(self)
}

object PlatformManager {
  private val INSTANTIATION_LOCK = new Object()

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[PlatformManager]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   */
  def getOrCreate: PlatformManager = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new PlatformManager()
      }
    }
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(platformManager: PlatformManager): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(platformManager)
    }
  }

  private def createSQLContextHolder(params: java.util.Map[Any, Any], runtime: StreamingRuntime) = {
    val sc = runtime match {
      case a: SparkStreamingRuntime => a.streamingContext.sparkContext
      case b: SparkRuntime => b.sparkContext
      case _ => throw new RuntimeException("get _runtime_ fail")
    }
    new SQLContextHolder(
      params.containsKey("streaming.enableHiveSupport") &&
        params.get("streaming.enableHiveSupport").toString.toBoolean, sc)
  }

  def getRuntime: StreamingRuntime = {
    val params: JMap[String, String] = getOrCreate.config.get().getParamsMap
    val tempParams: JMap[Any, Any] = params.map(f => (f._1.asInstanceOf[Any], f._2.asInstanceOf[Any]))

    val platformName = params.get("streaming.platform")
    val runtime = platformName match {
      case platform: String if platform == "spark" =>

        SparkRuntime.getOrCreate(tempParams)
      case platform: String if platform == "storm" =>
        null
      case _ => SparkStreamingRuntime.getOrCreate(tempParams)
    }
    if (SQLContextHolder.sqlContextHolder == null) {
      SQLContextHolder.setActive(createSQLContextHolder(tempParams, runtime))
      tempParams.put("_sqlContextHolder_", SQLContextHolder.getOrCreate())
    }

    runtime
  }

  def SPAKR_STREAMING = "spark_streaming"

  def STORM = "storm"

  def SPARK = "spark"

}


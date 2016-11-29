package streaming.core.strategy.platform

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.{List => JList, Map => JMap}

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import net.csdn.common.logging.Loggers
import org.apache.spark.SparkContext
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.zk.{ZKClient, ZkRegister}
import streaming.common.{ParamsUtil, SQLContextHolder, SparkCompatibility}
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

  def preCompile(runtime: StreamingRuntime) = {
    SparkCompatibility.preCompile(runtime)
  }

  def registerToZk(params: ParamsUtil) = {
    zk = ZkRegister.registerToZk(params)
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

    if (params.getBooleanParam("streaming.compatibility.crossversion", false)) {
      preCompile(runtime)
    }

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

    if (params.getBooleanParam("streaming.unitest.startRuntime", true)) {
      runtime.startRuntime
    }
    if (params.getBooleanParam("streaming.unitest.awaitTermination", true)) {
      runtime.awaitTermination
    }

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

  def getRuntimeContext(runtime: StreamingRuntime) = {

    runtime match {
      case s: SparkStreamingRuntime => s.streamingContext.sparkContext
      case s2: SparkRuntime => s2.sparkContext
      case _ =>
        Class.forName(runtime.getClass.getName).
          getMethod("sparkContext").
          invoke(runtime).asInstanceOf[SparkContext]
    }

  }

  def createRuntimeByPlatform(name: String, tempParams: java.util.Map[Any, Any]) = {
    Class.forName(name).
      getMethod("getOrCreate", classOf[JMap[Any, Any]]).
      invoke(null, tempParams).asInstanceOf[StreamingRuntime]
  }


  def clear = {
    lastInstantiatedContext.set(null)
  }

  def createSQLContextHolder(params: java.util.Map[Any, Any], runtime: StreamingRuntime) = {

    val sc = getRuntimeContext(runtime)

    if (params.containsKey("streaming.enableCarbonDataSupport")
      && params.get("streaming.enableCarbonDataSupport").toString.toBoolean
    ) {
      new SQLContextHolder(
        true, sc, Some(Map("className" -> "org.apache.spark.sql.CarbonContext",
          "store" -> params.getOrElse("streaming.carbondata.store","").toString,
          "meta" -> params.getOrElse("streaming.carbondata.meta","").toString)))
    } else {
      new SQLContextHolder(
        params.containsKey("streaming.enableHiveSupport") &&
          params.get("streaming.enableHiveSupport").toString.toBoolean, sc)
    }

  }

  def getRuntime: StreamingRuntime = {
    val params: JMap[String, String] = getOrCreate.config.get().getParamsMap
    val tempParams: JMap[Any, Any] = params.map(f => (f._1.asInstanceOf[Any], f._2.asInstanceOf[Any]))

    val platformName = params.get("streaming.platform")
    val runtime = platformName match {
      case platform: String if platform == SPARK =>
        SparkRuntime.getOrCreate(tempParams)

      case platform: String if platform == SPAKR_STREAMING =>
        SparkStreamingRuntime.getOrCreate(tempParams)

      case platform: String if platform == "storm" =>
        throw new RuntimeException("Platform storm is not supported yet")

      case _ =>
        createRuntimeByPlatform(platformNameMapping(platformName), tempParams)
    }
    if (SQLContextHolder.sqlContextHolder == null) {
      SQLContextHolder.setActive(createSQLContextHolder(tempParams, runtime))
      tempParams.put("_sqlContextHolder_", SQLContextHolder.getOrCreate())
    }

    runtime
  }

  def SPAKR_STREAMING = "spark_streaming"

  def SPAKR_STRUCTURED_STREAMING = "spark_structured_streaming"

  def SPAKR_S_S = "ss"

  def STORM = "storm"

  def SPARK = "spark"

  def platformNameMapping = Map[String, String](
    SPAKR_S_S -> "streaming.core.strategy.platform.SparkStructuredStreamingRuntime",
    SPAKR_STRUCTURED_STREAMING -> "streaming.core.strategy.platform.SparkStructuredStreamingRuntime"
  )

}


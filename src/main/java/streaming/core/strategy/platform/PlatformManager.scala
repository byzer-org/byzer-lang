package streaming.core.strategy.platform

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.{List => JList, Map => JMap}

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.ParamsUtil
import streaming.core.strategy.JobStrategy
import streaming.core.{Dispatcher, StreamingApp}

import scala.collection.JavaConversions._

/**
 * 4/27/16 WilliamZhu(allwefantasy@gmail.com)
 */


class PlatformManager {
  self =>
  val config = new AtomicReference[ParamsUtil]()

  def dispatcher: StrategyDispatcher[Any] = {
    Dispatcher.dispatcher
  }

  def startRestServer = {
    ServiceFramwork.scanService.setLoader(classOf[StreamingApp])
    ServiceFramwork.enableNoThreadJoin()
    Application.main(Array())
  }


  def run(_params: ParamsUtil, reRun: Boolean = false) = {

    if (!reRun) {
      config.set(_params)
    }

    val params = config.get()

    val lastSparkStreamingRuntimeInfo = if (reRun) {
      val tempRuntime = PlatformManager.getRuntime(params.getParam("streaming.name"), Map[Any, Any]())
      SparkStreamingRuntime.clearLastInstantiatedContext()
      Some(tempRuntime.streamingRuntimeInfo)
    } else None

    var jobs: Array[String] = dispatcher.strategies.filter(f => f._2.isInstanceOf[JobStrategy]).keys.toArray

    if (params.hasParam("streaming.jobs"))
      jobs = params.getParam("streaming.jobs").split(",")

    val tempParams = new java.util.HashMap[Any, Any]()
    params.getParamsMap.filter(f => f._1.startsWith("streaming.")).foreach { f => tempParams.put(f._1, f._2) }
    val runtime = PlatformManager.getRuntime(params.getParam("streaming.name"), tempParams)

    lastSparkStreamingRuntimeInfo match {
      case Some(ssri) =>
        runtime.streamingRuntimeInfo = ssri
        runtime.resetStreamingRuntimeInfo
      case None =>
    }

    val jobCounter = new AtomicInteger(0)
    jobs.foreach {
      jobName =>
        dispatcher.dispatch(Dispatcher.contextParams(jobName))
        val index = jobCounter.get()
        val inputStreamId = runtime.streamingRuntimeInfo.sparkStreamingOperator.inputStreamId(index)
        runtime.streamingRuntimeInfo.jobNameToInputStreamId.put(jobName, inputStreamId)
        jobCounter.incrementAndGet()
    }

    runtime.startRuntime
    if (params.getBooleanParam("streaming.rest", false) && !reRun) {
      startRestServer
    }

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

  private[platform] def setLastInstantiatedContext(sparkStreamingRuntime: PlatformManager): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sparkStreamingRuntime)
    }
  }

  def getRuntime(name: String, params: JMap[Any, Any]) = {
    SparkStreamingRuntime.getOrCreate(params)
  }

}


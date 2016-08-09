package streaming.core.strategy.platform


import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JList, Map => JMap}

import net.csdn.common.logging.Loggers
import org.apache.spark.{SparkConf, SparkContext, SparkRuntimeOperator}
import streaming.common.ParamsHelper._

import scala.collection.JavaConversions._

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener {
  self =>

  private val logger = Loggers.getLogger(classOf[SparkRuntime])

  def name = "SPARK"

  var sparkContext: SparkContext = createRuntime

  val sparkRuntimeOperator = new SparkRuntimeOperator(_params, sparkContext)

  var sparkRuntimeInfo = new SparkRuntimeInfo()

  def operator = sparkRuntimeOperator

  def createRuntime = {
    val conf = new SparkConf()
    params.filter(f => f._1.toString.startsWith("spark.")).foreach { f =>
      conf.set(f._1.toString, f._2.toString)
    }

    if (params.containsKey("streaming.master")) {
      conf.setMaster(params.get("streaming.master").toString)
    }

    conf.setAppName(params.get("streaming.name").toString)

    val tempContext = new SparkContext(conf)
    tempContext
  }

  override def startRuntime: StreamingRuntime = {
    this
  }

  override def awaitTermination: Unit = {
    if (params.paramAsBoolean("streaming.spark.service", false)) {
      Thread.currentThread().join()
    }
  }

  override def streamingRuntimeInfo: StreamingRuntimeInfo = sparkRuntimeInfo

  override def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean): Boolean = {
    sparkContext.stop()
    true
  }

  override def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo): Unit = {}

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator): Unit = {

  }

  override def params: JMap[Any, Any] = _params

  override def processEvent(event: Event): Unit = {}

  SparkRuntime.setLastInstantiatedContext(this)
}

class SparkRuntimeInfo extends StreamingRuntimeInfo {

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

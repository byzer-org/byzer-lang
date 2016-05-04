package streaming.core.strategy.platform

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JList, Map => JMap}

import net.csdn.common.logging.Loggers
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingInfoCollector}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * 4/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkStreamingRuntime(_params: JMap[Any, Any]) {

  self =>

  private val logger = Loggers.getLogger(classOf[SparkStreamingRuntime])

  def name = "SPARK_STREAMING"

  var streamingContext: StreamingContext = createRuntime


  var streamingRuntimeInfo: SparkStreamingRuntimeInfo = new SparkStreamingRuntimeInfo()


  def params = _params

  def createRuntime = {

    val conf = new SparkConf()
    params.filter(f => f._1.toString.startsWith("spark.")).foreach { f =>
      conf.set(f._1.toString, f._2.toString)
    }
    if (params.containsKey("streaming.master")) {
      conf.setMaster(params.get("streaming.master").toString)
    }
    conf.setAppName(params.get("streaming.name").toString)
    val duration = params.getOrElse("streaming.duration", "10").toString.toInt
    if (SparkStreamingRuntime.sparkContext.get() == null) {
      SparkStreamingRuntime.sparkContext.set(new SparkContext(conf))
    }
    new StreamingContext(SparkStreamingRuntime.sparkContext.get(), Seconds(duration))
  }

  def destroyRuntime(stopGraceful:Boolean,stopSparkContext: Boolean = false) = {



    logger.info("SparkStreamingRuntime stopping.....")
    val inputStreamIdToJobName = streamingRuntimeInfo.jobNameToInputStreamId.map(f => (f._2, f._1))
    streamingRuntimeInfo.collector.snapShotInputStreamState(streamingContext).foreach { inputStream =>
      logger.info(s"SparkStreamingRuntime save inputstream state:" +
        s" ${inputStreamIdToJobName(inputStream._1)} => ${inputStream._2}")
      streamingRuntimeInfo.jobNameToState.put(inputStreamIdToJobName(inputStream._1), inputStream._2)
    }

    streamingRuntimeInfo.jobNameToInputStreamId.clear()
    streamingRuntimeInfo.inputStreamIdToState.clear()

    streamingContext.stop(stopSparkContext, stopGraceful)
  }

  def startRuntime = {
    logger.info("SparkStreamingRuntime start.....")
    streamingRuntimeInfo.jobNameToState.foreach { f =>
      val jobName = f._1
      val inputStreamId = streamingRuntimeInfo.jobNameToInputStreamId.get(jobName)
      logger.info(s"SparkStreamingRuntime restore inputstream state:" +
        s" ${jobName} => ${f._2}")
      streamingRuntimeInfo.collector.setInputStreamState(streamingContext,inputStreamId, f._2)
    }
    streamingContext.start()
    streamingRuntimeInfo.jobNameToState.clear()
  }

  def awaitTermination = {

    streamingContext.awaitTermination()
  }

  SparkStreamingRuntime.setLastInstantiatedContext(self)
}

class SparkStreamingRuntimeInfo {
  val jobNameToInputStreamId = new ConcurrentHashMap[String, Int]()
  val jobNameToState = new ConcurrentHashMap[String, Any]()
  val inputStreamIdToState = new ConcurrentHashMap[String, (Int, Any)]()
  val collector: StreamingInfoCollector = new StreamingInfoCollector()
}

object SparkStreamingRuntime {

  var sparkContext = new AtomicReference[SparkContext]()

  private val INSTANTIATION_LOCK = new Object()

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[SparkStreamingRuntime]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   */
  def getOrCreate(params: JMap[Any, Any]): SparkStreamingRuntime = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new SparkStreamingRuntime(params)
      }
    }
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(sparkStreamingRuntime: SparkStreamingRuntime): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sparkStreamingRuntime)
    }
  }
}

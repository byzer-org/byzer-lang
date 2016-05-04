package streaming.core.strategy.platform

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JList, Map => JMap}

import net.csdn.common.logging.Loggers
import org.apache.spark.streaming.{Seconds, SparkStreamingOperator, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * 4/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkStreamingRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener {

  self =>

  private val logger = Loggers.getLogger(classOf[SparkStreamingRuntime])

  def name = "SPARK_STREAMING"

  var streamingContext: StreamingContext = createRuntime


  private var _streamingRuntimeInfo: SparkStreamingRuntimeInfo = new SparkStreamingRuntimeInfo(streamingContext)

  override def streamingRuntimeInfo = _streamingRuntimeInfo

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator) = {
    _streamingRuntimeInfo.sparkStreamingOperator = new SparkStreamingOperator(streamingContext)
  }

  override def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo) = {
    _streamingRuntimeInfo = streamingRuntimeInfo.asInstanceOf[SparkStreamingRuntimeInfo]
  }

  override def params = _params

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

    if (params.containsKey("streaming.checkpoint")) {
      val checkpoinDir = params.get("streaming.checkpoint").toString
      StreamingContext.getActiveOrCreate(checkpoinDir,
        () => {
          val ssc = new StreamingContext(conf, Seconds(duration))
          if (SparkStreamingRuntime.sparkContext.get() == null) {
            SparkStreamingRuntime.sparkContext.set(ssc.sparkContext)
          }
          ssc.checkpoint(checkpoinDir)
          ssc
        }
      )
    } else {
      if (SparkStreamingRuntime.sparkContext.get() == null) {
        SparkStreamingRuntime.sparkContext.set(new SparkContext(conf))
      }
      new StreamingContext(SparkStreamingRuntime.sparkContext.get(), Seconds(duration))
    }

  }

  override def destroyRuntime(stopGraceful: Boolean, stopSparkContext: Boolean = false) = {


    logger.info("SparkStreamingRuntime stopping.....")
    val inputStreamIdToJobName = _streamingRuntimeInfo.jobNameToInputStreamId.map(f => (f._2, f._1))
    _streamingRuntimeInfo.sparkStreamingOperator.snapShotInputStreamState().foreach { inputStream =>
      logger.info(s"SparkStreamingRuntime save inputstream state:" +
        s" ${inputStreamIdToJobName(inputStream._1)} => ${inputStream._2}")
      _streamingRuntimeInfo.jobNameToState.put(inputStreamIdToJobName(inputStream._1), inputStream._2)
    }

    _streamingRuntimeInfo.jobNameToInputStreamId.clear()
    _streamingRuntimeInfo.inputStreamIdToState.clear()

    streamingContext.stop(stopSparkContext, stopGraceful)
    true
  }

  override def startRuntime = {
    logger.info("SparkStreamingRuntime start.....")
    _streamingRuntimeInfo.jobNameToState.foreach { f =>
      val jobName = f._1
      val inputStreamId = _streamingRuntimeInfo.jobNameToInputStreamId.get(jobName)
      logger.info(s"SparkStreamingRuntime restore inputstream state:" +
        s" ${jobName} => ${f._2}")
      _streamingRuntimeInfo.sparkStreamingOperator.setInputStreamState(inputStreamId, f._2)
    }

    streamingContext.start()
    _streamingRuntimeInfo.jobNameToState.clear()
    this
  }

  override def awaitTermination = {
    streamingContext.awaitTermination()
  }

  SparkStreamingRuntime.setLastInstantiatedContext(self)

  override def processEvent(event: Event): Unit = {
    event match {
      case e: JobFlowGenerate =>
        val inputStreamId = streamingRuntimeInfo.sparkStreamingOperator.inputStreamId(e.index)
        streamingRuntimeInfo.jobNameToInputStreamId.put(e.jobName, inputStreamId)
      case _ =>
    }
  }
}

class SparkStreamingRuntimeInfo(ssc: StreamingContext) extends StreamingRuntimeInfo {
  val jobNameToInputStreamId = new ConcurrentHashMap[String, Int]()
  val jobNameToState = new ConcurrentHashMap[String, Any]()
  val inputStreamIdToState = new ConcurrentHashMap[String, (Int, Any)]()
  var sparkStreamingOperator: SparkStreamingOperator = new SparkStreamingOperator(ssc)
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
    PlatformManager.getOrCreate.register(lastInstantiatedContext.get())
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      PlatformManager.getOrCreate.unRegister(lastInstantiatedContext.get())
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(sparkStreamingRuntime: SparkStreamingRuntime): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sparkStreamingRuntime)
    }
  }
}

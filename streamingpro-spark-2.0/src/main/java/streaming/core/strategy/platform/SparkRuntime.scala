package streaming.core.strategy.platform

import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger
import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkRuntimeOperator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 30/3/2017.
  */
class SparkRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener {

  val logger = Logger.getLogger(getClass.getName)

  def name = "SPARK"

  var sparkSession: SparkSession = createRuntime


  def operator = {
    new SparkRuntimeOperator(sparkSession)
  }

  def createRuntime = {
    val conf = new SparkConf()
    params.filter(f => f._1.toString.startsWith("spark.")).foreach { f =>
      conf.set(f._1.toString, f._2.toString)
    }

    if (params.containsKey("streaming.master")) {
      conf.setMaster(params.get("streaming.master").toString)
    }

    conf.setAppName(params.get("streaming.name").toString)

    val sparkSession = SparkSession.builder().config(conf)
    if (params.containsKey("streaming.enableHiveSupport") &&
      params.get("streaming.enableHiveSupport").toString.toBoolean) {
      sparkSession.enableHiveSupport()
    }

    if (params.containsKey("streaming.enableCarbonDataSupport") &&
      params.get("streaming.enableCarbonDataSupport").toString.toBoolean) {
      val url = params.getOrElse("streaming.hive.PlatformManager.jdo.option.ConnectionURL", "").toString
      if (!url.isEmpty) {
        logger.info("set hive javax.jdo.option.ConnectionURL=" + url)
        sparkSession.config("javax.jdo.option.ConnectionURL", url)
      }
      val carbonBuilder = Class.forName("org.apache.spark.sql.CarbonSession$CarbonBuilder").
        getConstructor(classOf[SparkSession.Builder]).
        newInstance(sparkSession)
      Class.forName("org.apache.spark.sql.CarbonSession$CarbonBuilder").
        getMethod("getOrCreateCarbonSession", classOf[String], classOf[String]).
        invoke(carbonBuilder, params("streaming.carbondata.store").toString, params("streaming.carbondata.meta").toString).asInstanceOf[SparkSession]
    } else {
      sparkSession.getOrCreate()
    }
  }

  params.put("_session_", sparkSession)


  override def startRuntime: StreamingRuntime = {
    this
  }

  override def awaitTermination: Unit = {
    if (params.getOrElse("streaming.spark.service", false).toString.toBoolean) {
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
    HiveThriftServer2.startWithContext(sparkSession.sqlContext.asInstanceOf[HiveContext])
  }

  override def startHttpServer: Unit = {}

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

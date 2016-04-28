package streaming.core.strategy.platform

import java.util.{List => JList, Map => JMap}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * 4/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkStreamingRuntime(params: JMap[Any, Any]) {
  def name = "SPARK_STREAMING"

  val streamingContext = createRuntime


  def createRuntime = {

    val conf = new SparkConf()
    params.filter(f => f._1.toString.startsWith("spark.")).foreach { f =>
      conf.set(f._1.toString, f._2.toString)
    }
    if (params.containsKey("streaming.master")) {
      conf.setMaster(params.get("streaming.master").toString)
    }
    conf.setAppName(params.get("streaming.name").toString)
    val duration = conf.getInt("streaming.duration", 10)
    val sparkContext = new SparkContext(conf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(duration))
    streamingContext

  }

  def destroyRuntime = {
    streamingContext.stop(true, true)
  }

  def awaitTermination = {
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

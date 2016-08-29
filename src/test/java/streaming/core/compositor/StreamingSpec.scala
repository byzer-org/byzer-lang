package streaming.core.compositor

import org.apache.spark.streaming.{BasicStreamingOperation, BatchCounter}
import org.scalatest._
import streaming.common.ParamsUtil
import streaming.core.Dispatcher
import streaming.core.compositor.spark.streaming.output.SQLUnitTestCompositor
import streaming.core.strategy.platform.{PlatformManager, SparkStreamingRuntime}

import scala.collection.JavaConversions._

/**
 * 8/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
class StreamingSpec extends FlatSpec with Matchers with BasicStreamingOperation {


  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "true",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false")

  val streamingParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "true",
    "-streaming.platform", "spark_streaming",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-streaming.duration", "2",
    "-streaming.unitest.startRuntime", "false",
    "-streaming.unitest.awaitTermination", "false",
    "-spark.streaming.clock", "org.apache.spark.util.ManualClock"
  )


  "streaming" should "run only one batch then exist" in {

    withStreamingContext(setupStreamingContext("classpath:///strategy.v2.json")) { runtime: SparkStreamingRuntime =>
      val batchCounter = new BatchCounter(runtime.streamingContext)
      runtime.startRuntime
      val clock = manualClock(runtime.streamingContext)
      clock.advance(2000)
      batchCounter.waitUntilBatchesCompleted(1, 4000)
      val sd = Dispatcher.dispatcher(null)
      val strategies = sd.findStrategies("test").get

      strategies.size should be(1)

      val output = strategies.head.compositor.last.asInstanceOf[SQLUnitTestCompositor[Any]]
      val result = output.result.head

      result.size should be(1)

      result.head.getAs[String]("a") should be("3")
      result.head.getAs[String]("b") should be("5")

    }
  }

  def setupStreamingContext(configFilePath: String) = {
    val extraParam = Array("-streaming.job.file.path", configFilePath)
    val params = new ParamsUtil(streamingParams ++ extraParam)
    PlatformManager.getOrCreate.run(params, false)
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkStreamingRuntime]
    runtime
  }


}

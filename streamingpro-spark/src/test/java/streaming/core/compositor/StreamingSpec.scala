package streaming.core.compositor

import org.apache.spark.streaming.{BasicStreamingOperation, BatchCounter}
import streaming.core.Dispatcher
import streaming.core.compositor.spark.streaming.output.SQLUnitTestCompositor
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._

/**
  * 8/29/16 WilliamZhu(allwefantasy@gmail.com)
  */
class StreamingSpec extends BasicStreamingOperation {


  val streamingParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark_streaming",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-streaming.duration", "2",
    "-streaming.unitest.startRuntime", "false",
    "-streaming.unitest.awaitTermination", "false",
    "-spark.streaming.clock", "org.apache.spark.util.ManualClock"
  )


  "streaming with join table" should "run normally" in {

    withStreamingContext(setupStreamingContext(streamingParams, "classpath:///test/streaming-test.json")) { runtime: SparkStreamingRuntime =>
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

  "streaming source from map" should "convert correctly" in {

    withStreamingContext(setupStreamingContext(streamingParams, "classpath:///test/streaming-test-scalamaptojson.json")) { runtime: SparkStreamingRuntime =>
      val batchCounter = new BatchCounter(runtime.streamingContext)
      runtime.startRuntime
      val clock = manualClock(runtime.streamingContext)
      clock.advance(2000)
      batchCounter.waitUntilBatchesCompleted(1, 4000)
      val sd = Dispatcher.dispatcher(null)
      val strategies = sd.findStrategies("scalamaptojson").get

      strategies.size should be(1)

      val output = strategies.head.compositor.last.asInstanceOf[SQLUnitTestCompositor[Any]]
      val result = output.result.head

      result.size should be(1)

      result.head.getAs[String]("a") should be("yes")
    }
  }


}
package streaming.core.compositor

import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicReference

import com.google.common.io.Files
import org.apache.spark.streaming.BasicStreamingOperation
import serviceframework.dispatcher.StrategyDispatcher
import streaming.core.Dispatcher
import streaming.core.compositor.spark.output.SQLUnitTestCompositor
import streaming.core.strategy.platform.SparkRuntime

import scala.collection.JavaConversions._

/**
 * 8/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
class BatchSpec extends BasicStreamingOperation {


  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )


  "batch" should "run normally" in {
    val file = new java.io.File("/tmp/hdfsfile/abc.txt")
    Files.createParentDirs(file)
    Files.write("abc\tbbc", file, Charset.forName("utf-8"))

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/batch-test.json")) { runtime: SparkRuntime =>

      val sd = Dispatcher.dispatcher(null)
      val strategies = sd.findStrategies("convert_data_parquet").get
      strategies.size should be(1)

      val output = strategies.head.compositor.last.asInstanceOf[SQLUnitTestCompositor[Any]]
      val result = output.result.head

      result.size should be(1)

      result.head.getAs[String]("tp") should be ("bbc")

      file.delete()

    }
  }


}

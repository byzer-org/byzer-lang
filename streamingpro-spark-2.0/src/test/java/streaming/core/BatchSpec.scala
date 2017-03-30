package streaming.core

import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.compositor.spark.output.MultiSQLOutputCompositor
import streaming.core.strategy.platform.SparkRuntime

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 30/3/2017.
  */
class BatchSpec extends BasicSparkOperation {


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
    Files.write(s""" {"abc":"123","bbc":"adkfj"} """, file, Charset.forName("utf-8"))

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/batch-console.json")) { runtime: SparkRuntime =>

      val sd = Dispatcher.dispatcher(null)
      val strategies = sd.findStrategies("batch-console").get
      strategies.size should be(1)

      val output = strategies.head.compositor.last.asInstanceOf[MultiSQLOutputCompositor[_]]
      file.delete()

    }
  }
}

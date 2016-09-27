package streaming.core.compositor

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.streaming.BasicStreamingOperation
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

      result.head.getAs[String]("tp") should be("bbc")

      file.delete()

    }
  }

  "batch-with-inline-script" should "run normally" in {
    val file = new java.io.File("/tmp/hdfsfile/abc.txt")
    Files.createParentDirs(file)
    Files.write("kk\tbb", file, Charset.forName("utf-8"))

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/batch-test-inline-script.json")) { runtime: SparkRuntime =>

      val sd = Dispatcher.dispatcher(null)
      val strategies = sd.findStrategies("convert_data_parquet").get
      strategies.size should be(1)

      val output = strategies.head.compositor.last.asInstanceOf[SQLUnitTestCompositor[Any]]
      val result = output.result.head

      result.size should be(1)

      result.head.getAs[String]("a") should be("kk")

      println("a=>" + result.head.getAs[String]("a") + ",b=>" + result.head.getAs[String]("b"))

      file.delete()

    }
  }

  "batch-with-file-script" should "run normally" in {
    val file = new java.io.File("/tmp/hdfsfile/abc.txt")
    Files.createParentDirs(file)
    Files.write("kk\tbb", file, Charset.forName("utf-8"))

    val script =
      s""" val Array(a,b)=rawLine.split("\t")
           Map("a"->a,"b"->b)
       """.stripMargin
    val scripFile = new File("/tmp/raw_process.scala")
    Files.write(script, scripFile, Charset.forName("utf-8"))

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/batch-test-file-script.json")) { runtime: SparkRuntime =>

      val sd = Dispatcher.dispatcher(null)
      val strategies = sd.findStrategies("convert_data_parquet").get
      strategies.size should be(1)

      val output = strategies.head.compositor.last.asInstanceOf[SQLUnitTestCompositor[Any]]
      val result = output.result.head

      result.size should be(1)

      result.head.getAs[String]("a") should be("kk")

      println("a=>" + result.head.getAs[String]("a") + ",b=>" + result.head.getAs[String]("b"))

      file.delete()
      scripFile.delete()

    }
  }


  "batch-with-inline-script-doc" should "run normally" in {
    val file = new java.io.File("/tmp/hdfsfile/abc.txt")
    Files.createParentDirs(file)
    Files.write("kk\tbb", file, Charset.forName("utf-8"))

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/batch-test-script-doc.json")) { runtime: SparkRuntime =>

      val sd = Dispatcher.dispatcher(null)
      val strategies = sd.findStrategies("convert_data_parquet").get
      strategies.size should be(1)

      val output = strategies.head.compositor.last.asInstanceOf[SQLUnitTestCompositor[Any]]
      val result = output.result.head

      result.size should be(1)

      result.head.getAs[String]("a") should be("kk")

      println("a=>" + result.head.getAs[String]("a") + ",b=>" + result.head.getAs[String]("b"))

      file.delete()

    }
  }


}

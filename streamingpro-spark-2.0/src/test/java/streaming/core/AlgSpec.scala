package streaming.core

import java.io.File
import java.nio.charset.Charset
import java.util

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.BasicSparkOperation

import scala.collection.JavaConversions._
import streaming.core.Dispatcher
import streaming.core.compositor.spark.output.AlgorithmOutputCompositor
import streaming.core.compositor.spark.source.MultiSQLSourceCompositor
import streaming.core.strategy.platform.SparkRuntime

/**
  * Created by allwefantasy on 2/5/2017.
  */
class AlgSpec extends BasicSparkOperation {
  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )

  def cleanPath(path: String) = {
    if (path.startsWith("file://")) {
      path.substring("file://".length)
    }
    else path
  }

  "lr training" should "create model file" in {

    val contextParams: java.util.Map[Any, Any] = new util.HashMap[Any, Any]()
    contextParams.put("streaming.job.file.path", "classpath:///test/alg-lr.json")
    val sd = Dispatcher.dispatcher(contextParams)
    val source = sd.findStrategies("alg").get.head.compositor.head.asInstanceOf[MultiSQLSourceCompositor[Any]]
    val output = sd.findStrategies("alg").get.head.compositor.last.asInstanceOf[AlgorithmOutputCompositor[Any]]



    val filePath = getCompositorParam(source).head.get("path").toString
    val outputPath = getCompositorParam(output).head.get("path").toString

    new File(cleanPath(filePath)).delete()
    FileUtils.deleteDirectory(new File(cleanPath(outputPath)))

    val fileInput = scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("/data/mllib/sample_linear_regression_data.txt"))
    Files.write(fileInput.getLines().mkString("\n"), new File(cleanPath(filePath)), Charset.forName("utf-8"))

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/alg-lr.json")) { runtime: SparkRuntime =>
      assume(new File(cleanPath(outputPath)).exists())
    }

  }

}

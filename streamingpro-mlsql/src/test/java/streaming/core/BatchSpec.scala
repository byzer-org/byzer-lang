package streaming.core

import java.nio.charset.Charset

import com.google.common.io.Files
import net.sf.json.JSONObject
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.compositor.spark.output.MultiSQLOutputCompositor
import streaming.core.strategy.platform.SparkRuntime

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 30/3/2017.
  */
class BatchSpec extends BasicSparkOperation with SpecFunctions {


  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )

  val batchParamsWithCarbondata = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true",
    "-streaming.enableCarbonDataSupport", "true",
    "-streaming.carbondata.store", "/tmp/carbondata/store",
    "-streaming.carbondata.meta", "/tmp/carbondata/meta"
  )


  "batch-console" should "work fine" in {
    val file = new java.io.File("/tmp/hdfsfile/abc.txt")
    Files.createParentDirs(file)
    Files.write(s""" {"abc":"123","bbc":"adkfj"} """, file, Charset.forName("utf-8"))

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/batch-console.json")) { runtime: SparkRuntime =>

      val sd = Dispatcher.dispatcher(null)
      val strategies = sd.findStrategies("batch-console").get
      //      val output = strategies.head.compositor.last.asInstanceOf[MultiSQLOutputCompositor[_]]
      //      output
      file.delete()

    }
  }

  "batch-carbondata" should "work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithCarbondata, "classpath:///test/batch-cache-support.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      dropTables(Seq("download_carbon", "download_carbon2"))
      val sd = Dispatcher.dispatcher(null)

      //      val result = runtime.sparkSession.sql("select * from download_carbon").toJSON.collect()
      //      assume(result.size == 1)
      //      assume(JSONObject.fromObject(result(0)).getString("a") == "a")
    }
  }
}

package streaming.rest

import net.csdn.annotation.rest.At
import net.csdn.common.exception.RenderFinish
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method._
import org.apache.spark.sql.execution.streaming.{ForeachHttpSink, ForeachSink, SQLExecute}
import streaming.core.strategy.platform.{PlatformManager, SparkStructuredStreamingRuntime}

/**
  * Created by allwefantasy on 28/3/2017.
  */
class RestController extends ApplicationController {
  @At(path = Array("/run/sql"), types = Array(GET,POST))
  def ss = {
    if (!runtime.isInstanceOf[SparkStructuredStreamingRuntime]) {
      render(400, "runtime should be spark_structured_streaming")
    }

    val spark = runtime.asInstanceOf[SparkStructuredStreamingRuntime].sparkSession

    new SQLExecute(spark,restResponse).query(param("sql"))
    throw new RenderFinish()
  }

  def runtime = PlatformManager.getRuntime
}

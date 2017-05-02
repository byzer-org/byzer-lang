package streaming.common

import org.apache.spark.util.ScalaSourceCodeCompiler
import streaming.core.strategy.platform.{SparkRuntime, SparkStreamingRuntime, StreamingRuntime}

/**
  * 8/3/16 WilliamZhu(allwefantasy@gmail.com)
  */
object SparkCompatibility {

  def sparkVersion = {
    org.apache.spark.SPARK_VERSION
  }
}

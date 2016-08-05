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
  def preCompile(runtime: StreamingRuntime) = {
    val sc = runtime match {
      case a: SparkStreamingRuntime =>
        a.streamingContext.sparkContext
      case b: SparkRuntime => b.sparkContext
      case _ => throw new RuntimeException("get _runtime_ fail")
    }
    val version = sc.version

    if (version.startsWith("2")) {
      ScalaSourceCodeCompiler.compileCode2(
        CodeTemplates.spark_after_2_0_rest_json_source_string,
        CodeTemplates.spark_after_2_0_rest_json_source_clazz)
    }
    else {
      ScalaSourceCodeCompiler.compileCode2(
        CodeTemplates.spark_before_2_0_rest_json_source_string,
        CodeTemplates.spark_before_2_0_rest_json_source_clazz)
    }
  }
}

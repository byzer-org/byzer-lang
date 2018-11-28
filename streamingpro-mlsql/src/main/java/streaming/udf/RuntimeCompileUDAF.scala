package streaming.udf

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey

/**
 * Created by fchen on 2018/11/15.
 */
trait RuntimeCompileUDAF extends RuntimeCompileScriptInterface[UserDefinedAggregateFunction] {

  def udaf(e: Seq[Expression], scriptCacheKey: ScriptUDFCacheKey): ScalaUDAF = {
    ScalaUDAF(e, generateFunction(scriptCacheKey))
  }
}

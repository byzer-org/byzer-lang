package streaming.core

import java.net.URL

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry._
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.ScalaSourceCodeCompiler
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{FlatSpec, Matchers}
import streaming.common.{PunctuationUtils, ScriptCacheKey, SourceCodeCompiler, UnicodeUtils}

import scala.collection.mutable

/**
  * Created by allwefantasy on 28/3/2017.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val s =
      """
        |package jack
        |class PlusFun{
        |  def plusFun(a:Double,b:Double)={
        |   a + b
        |  }
        |}
      """.stripMargin
    ScalaSourceCodeCompiler.compileAndRun(s,Map())
    val clzz = SourceCodeCompiler.execute(ScriptCacheKey(s, "jack.PlusFun")).asInstanceOf[Class[_]]
    val method = UdfUtils.getMethod(clzz, "plusFun")
    val dataType: (DataType, Boolean) = JavaTypeInference.inferDataType(method.getReturnType)
    println(method)
    println(dataType)
  }
}

object UdfUtils {

  def newInstance(clazz: Class[_]): Any = {
    val constructor = clazz.getDeclaredConstructors.head
    constructor.setAccessible(true)
    constructor.newInstance()
  }

  def getMethod(clazz: Class[_], method: String) = {
    val candidate = clazz.getDeclaredMethods.filter(_.getName == method).filterNot(_.isBridge)
    if (candidate.isEmpty) {
      throw new Exception(s"No method $method found in class ${clazz.getCanonicalName}")
    } else if (candidate.length > 1) {
      throw new Exception(s"Multiple method $method found in class ${clazz.getCanonicalName}")
    } else {
      candidate.head
    }
  }

}

case class VeterxAndGroup(vertexId: VertexId, group: VertexId)

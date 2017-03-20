package org.apache.spark.util

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import streaming.common.CodeTemplates

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.AbstractFile
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.IMain
import scala.tools.reflect.ToolBox

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */

trait StreamingProGenerateClass {
 def execute(rawLine: String): Map[String, Any] = {
    Map[String, Any]()
  }

  def execute(doc: Map[String, Any]): Map[String, Any] = {
    Map[String, Any]()
  }

  def schema(): Option[StructType] = {
    None
  }

  def execute(context: SQLContext): Unit = {

  }
}




case class ScriptCacheKey(prefix: String, code: String)

object ScalaSourceCodeCompiler {


  def generateStreamingProGenerateClass(scriptCacheKey: ScriptCacheKey) = {
    val startTime = System.nanoTime()
    val function1 = if(scriptCacheKey.prefix =="rawLine") { s"""
                       |override  def execute(rawLine:String):Map[String,Any] = {
                       | ${scriptCacheKey.code}
        |}
              """.stripMargin } else  ""

    val function2 = if(scriptCacheKey.prefix =="doc") { s"""
                       |override  def execute(doc:Map[String,Any]):Map[String,Any] = {
                       | ${scriptCacheKey.code}
        |}
              """.stripMargin } else ""


    val function3 = if(scriptCacheKey.prefix =="schema") { s"""
                       |override  def schema():Option[StructType] = {
                       | ${scriptCacheKey.code}
        |}
              """.stripMargin } else ""

    val function4 = if(scriptCacheKey.prefix =="context") { s"""
                                                           |override  def execute(context: SQLContext): Unit = {
                                                           |
                                                           | ${scriptCacheKey.code}
                                                           |}
              """.stripMargin } else ""


    val wrapper = s"""
                     |import org.apache.spark.util.StreamingProGenerateClass
                     |import org.apache.spark.sql.SQLContext
                     |import org.apache.spark.sql.types._
                     |class StreamingProUDF_${startTime} extends StreamingProGenerateClass {
                                                          |
                                                          | ${function1}
        |
        | ${function2}
        |
        | ${function3}
        |
        | ${function4}
        |
        |}
        |new StreamingProUDF_${startTime}()
            """.stripMargin

    val result = compileCode(wrapper)

    result.asInstanceOf[StreamingProGenerateClass]
  }


  private val scriptCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[ScriptCacheKey, StreamingProGenerateClass]() {
        override def load(scriptCacheKey: ScriptCacheKey): StreamingProGenerateClass = {
          val startTime = System.nanoTime()
          val res = generateStreamingProGenerateClass(scriptCacheKey)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          res
        }
      })

  def execute(scriptCacheKey: ScriptCacheKey): StreamingProGenerateClass = {
    scriptCache.get(scriptCacheKey)
  }

  def compileCode(code: String): Any = {
    import scala.reflect.runtime.universe._
    val cm = runtimeMirror(Utils.getContextOrSparkClassLoader)
    val toolbox = cm.mkToolBox()
    val tree = toolbox.parse(code)
    val ref = toolbox.compile(tree)()
    ref
  }

  //
  //    def compileCode3[T](codeBody: String, references: Array[Any]): T = {
  //      val code = CodeFormatter.stripOverlappingComments(
  //        new CodeAndComment(codeBody, Map()))
  //
  //      val c = CodeGenerator.compile(code)
  //      c.generate(references).asInstanceOf[T]
  //    }

  def main(args: Array[String]): Unit = {
    val abc = CodeTemplates.spark_after_2_0_rest_json_source_string
    //compileCode2(abc)
  }


  private def findName(classP: ArrayBuffer[String], abstractFile: AbstractFile): Unit = {

    classP += abstractFile.name
    if (abstractFile.isDirectory && abstractFile.iterator.hasNext) {
      findName(classP, abstractFile.iterator.next())
    }
  }

  def compileCode2(code: String, classNames: Array[String]): Any = {
    val settings = new GenericRunnerSettings(error => sys.error(error))
    settings.usejavacp.value = true
    val interpreter = new IMain(settings)
    interpreter.compileString(code)
    classNames.foreach(interpreter.interpret(_))
    interpreter.close()
    true
  }
}

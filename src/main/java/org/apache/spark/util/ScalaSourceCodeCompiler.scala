package org.apache.spark.util

import com.google.common.cache.{CacheBuilder, CacheLoader}
import streaming.common.CodeTemplates

import scala.collection.Iterator
import scala.collection.convert.Wrappers.{IteratorWrapper, JIteratorWrapper}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.AbstractFile
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.IMain
import scala.tools.reflect.ToolBox

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */

trait StreamingProGenerateClass {
  def execute(rawLine: String): Map[String, Any]

  def execute(doc: Map[String, Any]): Map[String, Any]
}

case class ScriptCacheKey(prefix:String,code:String)

object ScalaSourceCodeCompiler {

  private val scriptCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[ScriptCacheKey, StreamingProGenerateClass]() {
        override def load(scriptCacheKey: ScriptCacheKey): StreamingProGenerateClass = {
          val startTime = System.nanoTime()

          val function1 =  s"""
                              |    def execute(rawLine:String):Map[String,Any] = {
                              |         ${if(scriptCacheKey.prefix != "rawLine") "Map[String,Any]()" else scriptCacheKey.code}
                              |    }
              """.stripMargin

          val function2 =  s"""
                              |    def execute(doc:Map[String,Any]):Map[String,Any] = {
                              |         ${if(scriptCacheKey.prefix == "rawLine") "Map[String,Any]()" else scriptCacheKey.code}
              |    }
              """.stripMargin

          val wrapper = s"""
                           import org.apache.spark.util.StreamingProGenerateClass

                           class StreamingProUDF_${startTime} extends StreamingProGenerateClass {
                              ${function1}

                              ${function2}
                           }
                           new StreamingProUDF_${startTime}()
            """

          val result = compileCode(wrapper)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000

          result.asInstanceOf[StreamingProGenerateClass]
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

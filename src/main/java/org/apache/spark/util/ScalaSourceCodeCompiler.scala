package org.apache.spark.util

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.Logging
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
  def execute(rawLine: String): Map[String, Any]
}

object ScalaSourceCodeCompiler extends Logging {

  private val scriptCache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[String, StreamingProGenerateClass]() {
        override def load(code: String):StreamingProGenerateClass  = {
          val startTime = System.nanoTime()
          val wrapper = s"""
                           import org.apache.spark.util.StreamingProGenerateClass

                           class StreamingProUDF_${startTime} extends StreamingProGenerateClass {
                             def execute(rawLine:String):Map[String,Any] = {
                                ${code}
                             }
                           }
                           new StreamingProUDF_${startTime}()
            """

          val result = compileCode(wrapper)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          logInfo(s"Code generated in $timeMs ms")
          result.asInstanceOf[StreamingProGenerateClass]
        }
      })

  def execute(code: String): StreamingProGenerateClass = {
    scriptCache.get(code)
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

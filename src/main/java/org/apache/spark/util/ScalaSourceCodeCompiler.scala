package org.apache.spark.util

import streaming.common.CodeTemplates

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.AbstractFile
import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.IMain
import scala.tools.reflect.ToolBox

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
object ScalaSourceCodeCompiler {


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

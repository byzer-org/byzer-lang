package org.apache.spark.util
import scala.tools.reflect.ToolBox

/**
 * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
 */
class ScalaSourceCodeCompiler {
  def compiledCode(code: String): Any = {
    import scala.reflect.runtime.universe._
    val cm = runtimeMirror(Utils.getContextOrSparkClassLoader)
    val toolbox = cm.mkToolBox()

    val tree = toolbox.parse(code)
    toolbox.compile(tree)()
  }
}

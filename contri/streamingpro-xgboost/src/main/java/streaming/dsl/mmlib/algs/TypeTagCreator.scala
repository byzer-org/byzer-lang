package streaming.dsl.mmlib.algs

import org.apache.spark.ml.Model

import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object TypeTagCreator {

  def TypeFromString[O](className: String): WeakTypeTag[Model[_]] = {
    createTypeTag(className)
  }

  private def createTypeTag(tp: String): WeakTypeTag[Model[_]] = {
    WeakTypeTag(currentMirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        val toolbox = currentMirror.mkToolBox()
        val ttagCall = s"scala.reflect.runtime.universe.weakTypeTag[$tp]"
        val tpe = toolbox.typecheck(toolbox.parse(ttagCall), toolbox.TYPEmode).tpe.resultType.typeArgs.head
        assert(m eq currentMirror, s"TypeTag[$tpe] defined in $currentMirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }
}

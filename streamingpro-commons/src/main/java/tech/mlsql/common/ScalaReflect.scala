package tech.mlsql.common

import scala.reflect.runtime.{universe => ru}

/**
  * 2019-05-01 WilliamZhu(allwefantasy@gmail.com)
  */
class ScalaReflect(obj: Any) {

  def this() = {
    this(null)
  }

  private def mirror = {
    ru.runtimeMirror(classOf[ScalaReflect].getClassLoader)
  }

  def toClass = {
    val classMirror = mirror.reflectClass(mirror.staticClass(obj.asInstanceOf[String]))
    //ru.typeOf[classMirror.type].decl(ru.termNames.CONSTRUCTOR).asMethod
  }

  def method[T](name: String) = {
    val x = mirror.reflect(obj)
    val y = x.symbol.typeSignature.member(ru.TermName(name))
    new ScalaMethodReflect(x, y.asMethod)
  }

  def module[T: ru.TypeTag](name: String) = {
    val x = mirror.reflectModule(ru.typeOf[T].termSymbol.asModule)
    val y = x.symbol.typeSignature.member(ru.TermName(name))
    new ScalaModuleReflect(x, y.asMethod)
  }

}

object ScalaReflect {

  def apply[T](obj: T): ScalaReflect = new ScalaReflect(obj)

  def apply[T](): ScalaReflect = new ScalaReflect()

  //def getClass[T: ru.TypeTag](obj: T) = ru.typeTag[T].tpe.typeSymbol.asClass


}

class ScalaModuleReflect(x: ru.ModuleMirror, y: ru.MethodSymbol) {
  private def mirror = {
    ru.runtimeMirror(getClass.getClassLoader)
  }

  def invoke(objs: Any*) = {
    val instance = x.instance
    mirror.reflect(instance).reflectMethod(y.asMethod)(objs)
  }
}

class ScalaMethodReflect(x: ru.InstanceMirror, y: ru.MethodSymbol) {
  def invoke(objs: Any*) = {
    x.reflectMethod(y.asMethod)(objs: _*)
  }
}

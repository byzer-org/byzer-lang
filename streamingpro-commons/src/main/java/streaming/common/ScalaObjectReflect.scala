package streaming.common

/**
  * Created by allwefantasy on 21/9/2018.
  */
object ScalaObjectReflect {
  def findObjectMethod(clzzName: String) = {
    val clzz = Class.forName(clzzName + "$")
    val instance = clzz.getField("MODULE$").get(null)
    (instance.getClass, instance)
  }
}

package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Params

/**
  * Created by allwefantasy on 13/1/2018.
  */
trait Functions {

  def configureModel(model: Params, params: Map[String, String]) = {
    model.params.map { f =>
      if (params.contains(f.name)) {
        val v = params(f.name)
        val m = model.getClass.getMethods.filter(m => m.getName == s"set${f.name.capitalize}").head
        val pt = m.getParameterTypes.head
        val v2 = pt match {
          case i if i.isAssignableFrom(classOf[Int]) => v.toInt
          case i if i.isAssignableFrom(classOf[Double]) => v.toDouble
          case i if i.isAssignableFrom(classOf[Float]) => v.toFloat
          case i if i.isAssignableFrom(classOf[Boolean]) => v.toBoolean
          case _ => v
        }
        m.invoke(model, v2.asInstanceOf[AnyRef])
      }
    }
  }

  def mapParams(name: String, params: Map[String, String]) = {
    params.filter(f => f._1.startsWith(name + ".")).map(f => (f._1.split("\\.").drop(1).mkString("."), f._2))
  }

  def arrayParams(name: String, params: Map[String, String]) = {
    params.filter(f => f._1.startsWith(name + ".")).map { f =>
      val Array(name, group, key) = f._1.split("\\.")
      (group, key, f._2)
    }.groupBy(f => f._1).map { f => f._2.map(k => (k._2, k._1)).toMap }.toArray
  }

  def getModelConstructField(model: Any, modelName: String, fieldName: String) = {
    val modelField = model.getClass.getDeclaredField("org$apache$spark$ml$feature$" + modelName + "$$" + fieldName)
    modelField.setAccessible(true)
    modelField.get(model)
  }

  def getModelField(model: Any,  fieldName: String) = {
    val modelField = model.getClass.getDeclaredField(fieldName)
    modelField.setAccessible(true)
    modelField.get(model)
  }
}

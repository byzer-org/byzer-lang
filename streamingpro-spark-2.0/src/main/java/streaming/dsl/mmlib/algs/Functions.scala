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
          case _ => v
        }
        m.invoke(model, v2.asInstanceOf[AnyRef])
      }
    }
  }

}

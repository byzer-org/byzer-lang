package streaming.common

import java.util.{Map => JMap}


/**
 * 5/14/16 WilliamZhu(allwefantasy@gmail.com)
 */
object ParamsHelper {
  implicit def mapToParams(_params: JMap[Any, Any]):Params = {
    new Params(_params)
  }
}

class Params(_params: JMap[Any, Any]) {
  def paramAsInt(key: String, defaultValue: Int = 0) = {
    if(_params.containsKey(key)){
      _params.get(key).toString.toInt
    }else
    defaultValue
  }

  def paramAsDouble(key: String, defaultValue: Double = 0) = {
    if(_params.containsKey(key)){
      _params.get(key).toString.toDouble
    }else
      defaultValue

  }

  def param(key: String, defaultValue: String = null) = {
    if(_params.containsKey(key)){
      _params.get(key).toString
    }else
      defaultValue

  }

  def paramAsBoolean(key: String, defaultValue: Boolean = false) = {
    if(_params.containsKey(key)){
      _params.get(key).toString.toBoolean
    }else
      defaultValue
  }

}


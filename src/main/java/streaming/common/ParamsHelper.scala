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
    _params.getOrDefault(key, defaultValue).toString.toInt
  }

  def paramAsDouble(key: String, defaultValue: Double = 0) = {
    _params.getOrDefault(key, defaultValue).toString.toDouble
  }

  def param(key: String, defaultValue: String = null) = {
    _params.getOrDefault(key, defaultValue).toString
  }

  def paramAsBoolean(key: String, defaultValue: Boolean = false) = {
    _params.getOrDefault(key, defaultValue).toString.toBoolean
  }

}


package streaming.core

import java.util

import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 27/3/2017.
  */
trait CompositorHelper {

  def config[T](name: String, _configParams: util.List[util.Map[Any, Any]]): Option[T] = {
    config(0, name, _configParams)
  }

  def config[T](index: Int, name: String, _configParams: util.List[util.Map[Any, Any]]) = {
    if (_configParams.size() > 0 && _configParams(0).containsKey(name)) {
      Some(_configParams(index).get(name).asInstanceOf[T])
    } else None
  }

  def translateSQL(_sql: String, params: util.Map[Any, Any]) = {
    var sql: String = _sql
    params.filter(_._1.toString.startsWith("streaming.sql.params.")).foreach { p =>
      val key = p._1.toString.split("\\.").last
      sql = sql.replaceAll(":" + key, p._2.toString)
    }
    sql
  }

  def sparkSession(params: util.Map[Any, Any]) = {
    params.get("_session_").asInstanceOf[SparkSession]
  }


}

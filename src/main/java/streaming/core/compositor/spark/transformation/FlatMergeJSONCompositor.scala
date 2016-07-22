package streaming.core.compositor.spark.transformation

import java.util

import net.sf.json.JSONObject
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.JSONPath

import scala.collection.JavaConversions._


/**
 * 6/13/16 WilliamZhu(allwefantasy@gmail.com)
 */
class FlatMergeJSONCompositor[T] extends Compositor[T] {

  def jsonKeyPaths = {
    _configParams.map { f =>
      f.map(f => (f._1.asInstanceOf[String], f._2.asInstanceOf[String])).toMap
    }
  }


  protected var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    val rdd = middleResult(0) match {
      case df: DataFrame => df.toJSON
      case rd: Any => rd.asInstanceOf[RDD[String]]
    }
    val _jsonKeyPaths = jsonKeyPaths

    val newRDD = rdd.flatMap { line =>


      val item = new util.HashMap[String, Object]()
      val basicProperty = List(_jsonKeyPaths.head)

      basicProperty.map { _jsonKeyPath =>

        _jsonKeyPath.foreach { kPath =>

          val key = kPath._1
          val path = kPath._2
          try {
            item.put(key, JSONPath.read(line, path).asInstanceOf[Object])
          } catch {
            case e: Exception =>
          }

        }
      }


      val subProperties = _jsonKeyPaths.subList(1, _jsonKeyPaths.length)

      subProperties.map { _jsonKeyPath =>
        val temp = new util.HashMap[String, Object]()
        _jsonKeyPath.map { kPath =>
          val path = kPath._2
          if (path.endsWith("_map_")) {
            val newPath = path.replaceAll("\\._map_", "")
            JSONPath.read(line, newPath).asInstanceOf[java.util.Map[String, java.util.Map[String, Object]]].map { k =>
              val newKey = newPath.substring(2,newPath.length).split("\\.").mkString("_")
              temp.put(newKey, k._1)
              temp.putAll(k._2)
              temp.putAll(item)
            }
          }
        }
        JSONObject.fromObject(temp).toString
      }
    }

    List(newRDD.asInstanceOf[T])
  }


}

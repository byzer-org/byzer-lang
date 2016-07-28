package streaming.common.json.parser

import java.util

import net.sf.json.{JSONArray, JSONObject}
import streaming.common.JSONPath

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 7/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object JSONParser {
  def apply(
             jsonStr: String,
             configs: List[Map[String, String]]
             ): List[String] = {
    new JSONParser(jsonStr, configs).render.toList
  }
}

class JSONParser(jsonStr: String,
                 configs: List[Map[String, String]]) {

  def render = {
    val subs = configs.subList(1, configs.length)
    val gMap = global(jsonStr)
    val res = subs.flatMap { subConfig =>
      val tempList = ArrayBuffer[String]()
      sub(jsonStr, subConfig) match {
        case Some(i) =>
          i.map { k =>
            val temp = new util.HashMap[String, Object]()
            temp.putAll(gMap)
            temp.putAll(k)
            tempList += JSONObject.fromObject(temp).toString()
          }
        case None =>
      }
      tempList
    }

    if (res.size > 0) res else List(JSONObject.fromObject(gMap).toString()).toBuffer

  }

  def sub(line: String, subConfig: Map[String, String]): Option[util.List[util.Map[String, Object]]] = {

    val temp = new util.HashMap[String, Object]()

    val path = subConfig("sub")
    val includePrefix = subConfig.get("includePrefix").map(_.toBoolean).getOrElse(false)

    path match {
      case s if path.endsWith("_map_") =>

        val newPath = path.replaceAll("\\._map_", "")

        val rawData:Any = JSONPath.read(line, newPath)
        var data: Any = rawData
        if (rawData.isInstanceOf[String]) {
          data = JSONObject.fromObject(rawData)
        } else {
          data = rawData
        }

        val res = data.asInstanceOf[java.util.Map[String, java.util.Map[String, Object]]].
          map { k =>
          val newKey = newPath.substring(2, newPath.length).split("\\.").mkString("_")
          temp.put(newKey, k._1)
          if (includePrefix) {
            k._2.map(f => temp.put(newKey + "_" + f._1, f._2))
          } else {
            temp.putAll(k._2)
          }
          temp
        }.toList
        Some(res)

      case s if path.endsWith("_array_") =>

        val newPath = path.replaceAll("\\._array_", "")

        val rawData:Any = JSONPath.read(line, newPath)
        var data: Any = rawData
        if (rawData.isInstanceOf[String]) {
          data = JSONArray.fromObject(rawData)
        } else {
          data = rawData
        }

        val res = data.
          asInstanceOf[java.util.List[java.util.Map[String, Object]]].
          map { k =>
          val newKey = newPath.substring(2, newPath.length).split("\\.").mkString("_")
          if (includePrefix) {
            k.map(f => temp.put(newKey + "_" + f._1, f._2))
          } else {
            temp.putAll(k)
          }
          temp
        }
        Some(res)

      case _ => None
    }

  }

  def global(line: String) = {
    val item = new util.HashMap[String, Object]()
    configs.head.foreach { kPath =>

      val key = kPath._1
      val path = kPath._2
      try {
        item.put(key, JSONPath.read(line, path).asInstanceOf[Object])
      } catch {
        case e: Exception =>
      }
    }
    item
  }
}



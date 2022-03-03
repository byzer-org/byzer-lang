package tech.mlsql.datasource.helper.rest

import com.jayway.jsonpath.JsonPath
import org.apache.spark.sql.mlsql.session.MLSQLException

/**
 * 3/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
trait PageStrategy {

  def nexPage(_content: Option[Any]): PageStrategy

  def hasNextPage(_content: Option[Any]): Boolean

  def pageUrl(_content: Option[Any]): String
}

object PageStrategy {
  val PAGE_CONFIG_VALUES = "config.page.values"

  def defaultHasNextPage(params: Map[String, String], _content: Option[Any]): Boolean = {
    val stopPagingCondition = params.get("config.page.stop")

    if (stopPagingCondition.isEmpty) {
      //todo: Choose a better default value
      return true
    }
    val content = _content.get.toString
    val Array(func, jsonPath) = stopPagingCondition.get.split(":")

    func match {
      case "size-zero" | "sizeZero" =>
        try {
          val targetValue = JsonPath.read[net.minidev.json.JSONArray](content, jsonPath)
          targetValue.size() > 0
        } catch {
          case _: com.jayway.jsonpath.PathNotFoundException =>
            false
        }

      case "not-exists" | "notExists" => try {
        JsonPath.read[Object](content, jsonPath)
        true
      } catch {
        case _: com.jayway.jsonpath.PathNotFoundException =>
          false
      }
      case "equals" =>
        try {
          val Array(realJsonPath, equalValue) = jsonPath.split(",")
          val targetValue = JsonPath.read[Object](content, realJsonPath).toString
          targetValue != equalValue
        } catch {
          case _: com.jayway.jsonpath.PathNotFoundException =>
            true
        }

      case _ => throw new MLSQLException(s"config.page.stop with ${func} is not supported")
    }
  }
}

package tech.mlsql.datasource.helper.rest

import com.jayway.jsonpath.JsonPath
import tech.mlsql.tool.Templates2

/**
 * 3/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class DefaultPageStrategy(params: Map[String, String]) extends PageStrategy {

  override def pageValues(_content: Option[Any]): Array[String] = {
    try {
      val content = _content.get.toString
      params("config.page.values").split(",").map(path => JsonPath.read[String](content, path)).toArray
    } catch {
      case _: com.jayway.jsonpath.PathNotFoundException =>
        Array[String]()
      case e: Exception =>
        throw e
    }
  }

  override def nexPage: DefaultPageStrategy = {
    this
  }

  override def pageUrl(_content: Option[Any]): String = {
    val urlTemplate = params("config.page.next")
    Templates2.evaluate(urlTemplate, pageValues(_content))
  }
}

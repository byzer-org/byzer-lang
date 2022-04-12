package tech.mlsql.datasource.helper.rest

import com.jayway.jsonpath.JsonPath
import tech.mlsql.tool.Templates2

/**
 * 3/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class DefaultPageStrategy(params: Map[String, String]) extends PageStrategy {
  /**
   * Extracts page values from content by jsonPath.
   * @param _content
   * @return
   */
  def pageValues(_content: Option[Any]): Array[String] = {
    try {
      val content = _content.get.toString
      params("config.page.values").split(",").map(path => JsonPath.read[Object](content, path).toString).toArray
    } catch {
      case _: com.jayway.jsonpath.PathNotFoundException =>
        Array[String]()
      case e: Exception =>
        throw e
    }
  }

  override def nexPage(_content: Option[Any]): DefaultPageStrategy = {
    this
  }

  override def pageUrl(_content: Option[Any]): String = {
    val urlTemplate = params("config.page.next")
    Templates2.evaluate(urlTemplate, pageValues(_content))
  }

  override def hasNextPage(_content: Option[Any]): Boolean = {
    params.get("config.page.stop") match {
      case Some(_) =>
        PageStrategy.defaultHasNextPage(params, _content)
      case None =>
        val pageValues = this.pageValues(_content)
        !(pageValues.size == 0 || pageValues.exists(value => value == null || value.isEmpty))
    }
  }
}

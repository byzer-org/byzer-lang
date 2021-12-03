package tech.mlsql.datasource.helper.rest

import tech.mlsql.tool.Templates2

/**
 * 3/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class AutoIncrementPageStrategy(params: Map[String, String]) extends PageStrategy {
  val Array(_, initialPageNum) = params("config.page.values").split(":")
  var pageNum = initialPageNum.toInt

  override def pageValues(_content: Option[Any]) = Array[String](pageNum.toString)


  override def nexPage: AutoIncrementPageStrategy = {
    pageNum += 1
    this
  }

  override def pageUrl(_content: Option[Any]): String = {
    val urlTemplate = params("config.page.next")
    Templates2.evaluate(urlTemplate, pageValues(None))
  }

}

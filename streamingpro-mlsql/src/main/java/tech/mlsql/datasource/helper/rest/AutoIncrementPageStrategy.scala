package tech.mlsql.datasource.helper.rest

import tech.mlsql.tool.Templates2

/**
 * 3/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class AutoIncrementPageStrategy(params: Map[String, String]) extends PageStrategy {
  // config.page.values="auto-increment:0"
  // config.page.stop="sizeZero:$.content"
  val Array(_, initialPageNum) = {
    if (!params.contains(PageStrategy.PAGE_CONFIG_VALUES)) {
      Array("", "0")
    } else {
      params("config.page.values").split(":")
    }

  }
  var pageNum = initialPageNum.toInt

  override def nexPage(_content: Option[Any]): AutoIncrementPageStrategy = {
    pageNum += 1
    this
  }

  override def pageUrl(_content: Option[Any]): String = {
    val urlTemplate = params("config.page.next")
    Templates2.evaluate(urlTemplate, Array(pageNum.toString))
  }

  override def hasNextPage(_content: Option[Any]): Boolean = {
    PageStrategy.defaultHasNextPage(params, _content)
  }
}

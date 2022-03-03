package tech.mlsql.datasource.helper.rest

import org.apache.spark.sql.mlsql.session.MLSQLException
import tech.mlsql.tool.Templates2

class OffsetPageStrategy(params: Map[String, String]) extends PageStrategy {
  val Array(_, pageParam) = params("config.page.values") match {
    case s if s.contains(":") => s.split(":")
    case _ => throw new MLSQLException("`config.page.values` should contains ':'")
  }

  val Array(initialPageNum, pageIncrement) = pageParam match {
    case s if s.contains(",") => pageParam.split(",")
    case _ => Array(pageParam, "1")
  }

  var pageNum: Int = initialPageNum.trim.toInt
  val pageAddend: Int = pageIncrement.trim.toInt

  override def nexPage(_content: Option[Any]): OffsetPageStrategy = {
    pageNum += pageAddend
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

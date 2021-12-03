package tech.mlsql.datasource.helper.rest

/**
 * 3/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
trait PageStrategy {
  def pageValues(_content: Option[Any]): Array[String]

  def nexPage: PageStrategy

  def pageUrl(_content: Option[Any]): String
}

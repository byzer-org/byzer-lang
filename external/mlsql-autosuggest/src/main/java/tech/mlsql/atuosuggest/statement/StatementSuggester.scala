package tech.mlsql.atuosuggest.statement

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait StatementSuggester {
  def name: String

  def isMatch(): Boolean

  def suggest(): List[String]
}

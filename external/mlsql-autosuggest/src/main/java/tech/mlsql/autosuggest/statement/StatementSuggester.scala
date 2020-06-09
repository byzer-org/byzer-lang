package tech.mlsql.autosuggest.statement

import tech.mlsql.autosuggest.meta.MetaTable

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait StatementSuggester {
  def name: String

  def isMatch(): Boolean

  def suggest(): List[SuggestItem]
}

case class SuggestItem(name: String, metaTable: MetaTable, extra: Map[String, String])

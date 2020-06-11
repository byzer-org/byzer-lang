package tech.mlsql.autosuggest.statement

import tech.mlsql.autosuggest.meta.MetaTable
import tech.mlsql.common.utils.log.Logging

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait StatementSuggester extends Logging{
  def name: String

  def isMatch(): Boolean

  def suggest(): List[SuggestItem]

  def defaultSuggest(subInstances: Map[String, StatementSuggester]): List[SuggestItem] = {
    var instance: StatementSuggester = null
    subInstances.foreach { _instance =>
      if (instance == null && _instance._2.isMatch()) {
        instance = _instance._2
      }
    }
    if (instance == null) List()
    else instance.suggest()

  }
}

case class SuggestItem(name: String, metaTable: MetaTable, extra: Map[String, String])

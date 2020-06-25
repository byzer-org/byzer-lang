package tech.mlsql.autosuggest

import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableKey}

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object SpecialTableConst {
  val KEY_WORD = "__KEY__WORD__"
  val DATA_SOURCE_KEY = "__DATA__SOURCE__"
  val OPTION_KEY = "__OPTION__"
  val TEMP_TABLE_DB_KEY = "__TEMP_TABLE__"

  val OTHER_TABLE_KEY = "__OTHER__TABLE__"

  val TOP_LEVEL_KEY = "__TOP_LEVEL__"

  def KEY_WORD_TABLE = MetaTable(MetaTableKey(None, None, SpecialTableConst.KEY_WORD), List())

  def DATA_SOURCE_TABLE = MetaTable(MetaTableKey(None, None, SpecialTableConst.DATA_SOURCE_KEY), List())

  def OPTION_TABLE = MetaTable(MetaTableKey(None, None, SpecialTableConst.OPTION_KEY), List())

  def OTHER_TABLE = MetaTable(MetaTableKey(None, None, SpecialTableConst.OTHER_TABLE_KEY), List())

  def tempTable(name: String) = MetaTable(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), name), List())

  def subQueryAliasTable = {
    MetaTableKey(None, None, null)
  }
}

package tech.mlsql.autosuggest

import tech.mlsql.autosuggest.meta.MetaTable

/**
 * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait FuncReg {
  def register: MetaTable
}

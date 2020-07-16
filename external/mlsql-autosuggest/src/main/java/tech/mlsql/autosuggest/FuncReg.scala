package tech.mlsql.autosuggest

import tech.mlsql.autosuggest.meta.MetaTable

/**
 * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait FuncReg {
  val DOC = "doc"
  val COLUMN = "column"
  val IS_AGG = "agg"
  val YES = "yes"
  val NO = "no"
  val DEFAULT_VALUE = "default"

  def register: MetaTable
}



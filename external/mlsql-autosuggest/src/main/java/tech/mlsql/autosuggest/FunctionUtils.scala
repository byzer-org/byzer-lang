package tech.mlsql.autosuggest

import tech.mlsql.autosuggest.SQLFunction.DB_KEY
import tech.mlsql.autosuggest.meta.{MetaTableColumn, MetaTableKey}

/**
 * 8/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object SQLFunction {
  val DB_KEY = "__FUNC__"
  val RETURN_KEY = "__RETURN__"

  def apply(name: String): SQLFunction = new SQLFunction()


}

class SQLFunction {
  def funcName(name: String) = {
    MetaTableKey(None, Option(DB_KEY), name)
    this
  }

  def param() = {
    
  }

  def funcParam(name: String, dataType: String) = {
    MetaTableColumn(name, dataType, true, Map())
  }

  def funcParam(name: String, dataType: String, isNull: Boolean) = {
    MetaTableColumn(name, dataType, true, Map())
  }
}

object DataType {
  val STRING = "string"
  val INT = "integer"
  val LONG = "long"
  val DOUBLE = "double"
  val DATE = "date"
  val DATE_TIMESTAMP = "date_timestamp"
}

package tech.mlsql.autosuggest

import tech.mlsql.autosuggest.MLSQLSQLFunction.DB_KEY
import tech.mlsql.autosuggest.meta.{MetaProvider, MetaTable, MetaTableColumn, MetaTableKey}

import scala.collection.mutable.ArrayBuffer


object MLSQLSQLFunction {
  val DB_KEY = "__FUNC__"
  val RETURN_KEY = "__RETURN__"
  val funcMetaProvider = new FuncMetaProvider()

  def apply(name: String): MLSQLSQLFunction = new MLSQLSQLFunction(name)
}

class MLSQLSQLFunction(name: String) {

  private val _params = ArrayBuffer[MetaTableColumn]()
  private val _returnParam = ArrayBuffer[MetaTableColumn]()
  private var _tableKey: MetaTableKey = MetaTableKey(None, Option(DB_KEY), name)
  private val _funcDescParam = ArrayBuffer[MetaTableColumn]()

  def funcName(name: String) = {
    _tableKey = MetaTableKey(None, Option(DB_KEY), name)
    this
  }

  def funcParam = {
    new MLSQLFuncParam(this)
  }

  def desc(extra: Map[String, String]) = {
    assert(_funcDescParam.size == 0, "desc can only invoke once")
    _funcDescParam += MetaTableColumn(MLSQLSQLFunction.DB_KEY, "", false, extra)
    this
  }

  def returnParam(dataType: String, isNull: Boolean, extra: Map[String, String]) = {
    assert(_returnParam.size == 0, "returnParam can only invoke once")
    _returnParam += MetaTableColumn(MLSQLSQLFunction.RETURN_KEY, dataType, isNull, extra)
    this
  }

  def addColumn(column: MetaTableColumn) = {
    _params += column
    this
  }

  def build = {
    MetaTable(_tableKey, (_funcDescParam ++ _returnParam ++ _params).toList)
  }

}

class MLSQLFuncParam(_func: MLSQLSQLFunction) {
  def param(name: String, dataType: String) = {
    _func.addColumn(MetaTableColumn(name, dataType, true, Map()))
    this
  }

  def param(name: String, dataType: String, isNull: Boolean) = {
    _func.addColumn(MetaTableColumn(name, dataType, isNull, Map()))
    this
  }

  def param(name: String, dataType: String, isNull: Boolean, extra: Map[String, String]) = {
    _func.addColumn(MetaTableColumn(name, dataType, isNull, extra))
    this
  }

  def func = {
    _func
  }
}

class FuncMetaProvider extends MetaProvider {
  private val funcs = scala.collection.mutable.HashMap[MetaTableKey, MetaTable]()

  override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
    funcs.get(key)
  }

  override def list(extra: Map[String, String] = Map()): List[MetaTable] = {
    funcs.map(_._2).toList
  }

  def register(func: MetaTable) = {
    this.funcs.put(func.key, func)
    this
  }

  // for test
  def clear = {
    funcs.clear()
  }
}


object DataType {
  val STRING = "string"
  val INT = "integer"
  val LONG = "long"
  val DOUBLE = "double"
  val NUMBER = "number"
  val DATE = "date"
  val DATE_TIMESTAMP = "date_timestamp"
  val ARRAY = "array"
  val MAP = "map"
}

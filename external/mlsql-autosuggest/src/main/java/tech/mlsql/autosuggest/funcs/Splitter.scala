package tech.mlsql.autosuggest.funcs

import tech.mlsql.autosuggest.{DataType, FuncReg, MLSQLSQLFunction}

/**
 * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class Splitter extends FuncReg {

  override def register = {
    val func = MLSQLSQLFunction.apply("split").
      funcParam.
      param("str", DataType.STRING).
      param("pattern", DataType.STRING).
      func.
      returnParam(DataType.ARRAY, true, Map()).
      build
    func
  }

}

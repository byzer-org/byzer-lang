package tech.mlsql.autosuggest.funcs

import tech.mlsql.autosuggest.{DataType, FuncReg, MLSQLSQLFunction}

/**
 * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class Splitter extends FuncReg {

  override def register = {
    val func = MLSQLSQLFunction.apply("split").
      funcParam.
      param("str", DataType.STRING, false, Map("zhDoc" -> "待切割字符")).
      param("pattern", DataType.STRING, false, Map("zhDoc" -> "分隔符")).
      func.
      returnParam(DataType.ARRAY, true, Map(
        "zhDoc" ->
          """
            |split函数。用于切割字符串，返回字符串数组
            |""".stripMargin
      )).
      build
    func
  }

}

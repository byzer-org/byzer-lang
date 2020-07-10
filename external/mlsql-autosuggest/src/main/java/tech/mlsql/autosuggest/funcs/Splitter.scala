package tech.mlsql.autosuggest.funcs

import tech.mlsql.autosuggest.{DataType, FuncReg, MLSQLSQLFunction}

/**
 * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class Splitter extends FuncReg {

  override def register = {
    val func = MLSQLSQLFunction.apply("split").desc(Map(
      "zhDoc" ->
        """
          |split函数。用于切割字符串，返回字符串数组. 
          |示例: split("a,b",",") == [a,b]
          |""".stripMargin
    )).
      funcParam.
      param("str", DataType.STRING, false, Map("zhDoc" -> "待切割字符", COLUMN -> YES)).
      param("pattern", DataType.STRING, false, Map("zhDoc" -> "分隔符", DEFAULT_VALUE -> ",")).
      func.
      returnParam(DataType.ARRAY, true, Map(
        "zhDoc" ->
          """
            |返回值是数组类型
            |""".stripMargin
      )).
      build
    func
  }

}

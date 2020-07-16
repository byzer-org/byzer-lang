package tech.mlsql.autosuggest.funcs

import tech.mlsql.autosuggest.{DataType, FuncReg, MLSQLSQLFunction}

/**
 * 14/7/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class Count extends FuncReg {
  override def register = {

    val func = MLSQLSQLFunction.apply("count").desc(Map(
      "zhDoc" ->
        """
          |count，统计数目，可单独或者配合group by 使用
          |""".stripMargin,
       IS_AGG -> YES
    )).
      funcParam.
      param("column", DataType.STRING, false, Map("zhDoc" -> "列名或者*或者常熟",COLUMN->YES)).
      func.
      returnParam(DataType.NUMBER, true, Map(
        "zhDoc" ->
          """
            |long类型数字
            |""".stripMargin
      )).
      build
    func
  }
}

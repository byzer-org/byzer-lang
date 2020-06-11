package com.intigua.antlr4.autosuggest

/**
 * 11/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SelectTest extends BaseTestWithoutSparkSession {
  test("spark sql test") {

    val items = context.buildFromString(
      """
        | -- yes
        | select  from (select a,b,c from table1) as table2;
        |""".stripMargin).suggest(3, 8)

    println(items)

  }
}

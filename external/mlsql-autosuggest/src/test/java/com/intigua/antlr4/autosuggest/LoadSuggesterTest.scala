package com.intigua.antlr4.autosuggest

import tech.mlsql.atuosuggest.statement.LoadSuggester
import tech.mlsql.atuosuggest.{TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class LoadSuggesterTest extends BaseTest {
  test("load hiv[cursor]") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hiv
        |""".stripMargin).tokens.asScala.toList
    val loadSuggester = new LoadSuggester(context, wow, TokenPos(1, TokenPosType.CURRENT, 3)).suggest()
    assert(loadSuggester.toList == List("hive"))
  }

  test("load [cursor]") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load 
        |""".stripMargin).tokens.asScala.toList
    val loadSuggester = new LoadSuggester(context, wow, TokenPos(0, TokenPosType.NEXT, 0)).suggest()
    println(loadSuggester)
    assert(loadSuggester.size > 1)
  }

 
}

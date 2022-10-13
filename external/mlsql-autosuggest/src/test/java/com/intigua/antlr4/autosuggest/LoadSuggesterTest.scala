package com.intigua.antlr4.autosuggest

import tech.mlsql.autosuggest.statement.LoadSuggester
import tech.mlsql.autosuggest.{TokenPos, TokenPosType}
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class LoadSuggesterTest extends BaseTest with Logging {
  test("load hiv[cursor]") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hiv
        |""".stripMargin).tokens.asScala.toList
    val loadSuggester = new LoadSuggester(context, wow, TokenPos(1, TokenPosType.CURRENT, 3)).suggest()
    assert(loadSuggester.map(_.name) == List("hive"))
  }

  test("load [cursor]") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load 
        |""".stripMargin).tokens.asScala.toList
    val loadSuggester = new LoadSuggester(context, wow, TokenPos(0, TokenPosType.NEXT, 0)).suggest()
    if (log.isInfoEnabled()) {
      var loadSuggesterToString = "";
      loadSuggester.foreach(i =>
        loadSuggesterToString += ("name: " + i.name + ",")
      )
      log.info(loadSuggesterToString)
    }
    assert(loadSuggester.size > 1)
  }

  test("load csv.`` where [cursor]") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load csv.`` where
        |""".stripMargin).tokens.asScala.toList
    val result = new LoadSuggester(context, wow, TokenPos(4, TokenPosType.NEXT, 0)).suggest()
    if (log.isInfoEnabled()) {
      log.info(result.toString())
    }

  }


}

package com.intigua.antlr4.autosuggest

import streaming.core.datasource.DataSourceRegistry
import tech.mlsql.autosuggest.statement.SaveSuggester
import tech.mlsql.autosuggest.{TokenPos, TokenPosType}

import scala.collection.JavaConverters._

class SaveSuggesterTest extends BaseTest {
  test("save i[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | save i
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SaveSuggester(context, statement, TokenPos(1, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("overwrite", "append", "errorIfExists", "ignore"))
  }

  test("save table1 as js[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | save table1 as js
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SaveSuggester(context, statement, TokenPos(3, TokenPosType.CURRENT, 2)).suggest()
    assert(suggestions.map(_.name).toSet == Set("json", "jsonStr"))
  }

  test("save table1 as csv.`1.csv` where e[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | save table1 as csv.`1.csv` where e
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SaveSuggester(context, statement, TokenPos(7, TokenPosType.CURRENT, 1)).suggest()
//    assert(suggestions.map(_.name).toSet == Set())
  }
}

package com.intigua.antlr4.autosuggest

import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher}
import tech.mlsql.autosuggest.statement.LexerUtils

import scala.collection.JavaConverters._

/**
 * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MatchTokenTest extends BaseTest {
  test("orIndex back") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select a.k from jack.drugs_bad_case_di as a
        |""".stripMargin).tokens.asScala.toList

    val tokens = LexerUtils.toRawSQLTokens(context, wow)
    val temp = TokenMatcher(tokens, 6).back.orIndex(Array(Food(None, SqlBaseLexer.FROM), Food(None, SqlBaseLexer.SELECT)))
    assert(temp == 4)
  }

  test("orIndex forward") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select a.k from jack.drugs_bad_case_di as a
        |""".stripMargin).tokens.asScala.toList

    val tokens = LexerUtils.toRawSQLTokens(context, wow)
    val temp = TokenMatcher(tokens, 0).forward.orIndex(Array(Food(None, SqlBaseLexer.FROM), Food(None, SqlBaseLexer.SELECT)))
    assert(temp == 0)
  }
}

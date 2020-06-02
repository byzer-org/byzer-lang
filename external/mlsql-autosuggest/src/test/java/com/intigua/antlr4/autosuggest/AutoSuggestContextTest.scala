package com.intigua.antlr4.autosuggest

import tech.mlsql.atuosuggest.statement.LexerUtils
import tech.mlsql.atuosuggest.{TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AutoSuggestContextTest extends BaseTest {
  test("parse") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`` as -- jack
        | table1;
        | select * from table1 as table2;
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)

    assert(context.statements.size == 2)

  }
  test("parse partial") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`` as -- jack
        | table1;
        | select * from table1
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)

    assert(context.statements.size == 2)
    println(context.statements)

  }

  test("relative pos convert") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`` as -- jack
        | table1;
        | select * from table1
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)

    assert(context.statements.size == 2)
    // select * f[cursor]rom table1
    val tokenPos = LexerUtils.toTokenPos(wow, 5, 11)
    assert(tokenPos == TokenPos(9, TokenPosType.CURRENT, 1))
    assert(context.toRelativePos(tokenPos)._1 == TokenPos(2, TokenPosType.CURRENT, 1))
  }

  test("keyword") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | loa
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)
    val tokenPos = LexerUtils.toTokenPos(wow, 3, 4)
    assert(tokenPos == TokenPos(0, TokenPosType.CURRENT, 3))
    assert(context.suggest(tokenPos)(0) == "load")
  }
}

package com.intigua.antlr4.autosuggest

import tech.mlsql.autosuggest.statement.ConnectSuggester
import tech.mlsql.autosuggest.{TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 8/11/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class ConnectSuggesterTest extends BaseTest {
  test("connect jd") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jd
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new ConnectSuggester(context, statement, TokenPos(1, TokenPosType.CURRENT, 2)).suggest()
    println(suggestions.map(_.name).toSet)
  }
  test("connect jdbc where `drive") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jdbc where `drive
        |""".stripMargin).tokens.asScala.toList

    val suggester = new ConnectSuggester(context, statement, TokenPos(4, TokenPosType.CURRENT, 5))
    println(suggester.suggest())
    //    println(suggestions.map(_.name).toSet)
  }
  test("connect jdbc where dri") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jdbc where dri
        |""".stripMargin).tokens.asScala.toList

    val suggester = new ConnectSuggester(context, statement, TokenPos(3, TokenPosType.CURRENT, 3))
    println(suggester.suggest())
    //    println(suggestions.map(_.name).toSet)
  }

  test("connect jdbc where ") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jdbc where 
        |""".stripMargin).tokens.asScala.toList

    val suggester = new ConnectSuggester(context, statement, TokenPos(2, TokenPosType.NEXT, 0))
    println(suggester.suggest())
    //    println(suggestions.map(_.name).toSet)
  }
  test("connect jdbc where driver= ") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jdbc where driver=
        |""".stripMargin).tokens.asScala.toList

    val suggester = new ConnectSuggester(context, statement, TokenPos(4, TokenPosType.NEXT, 0))
    println(suggester.suggest())
    //    println(suggestions.map(_.name).toSet)
  }

  test("connect jdbc where driver=\"\" ") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jdbc where driver="com"
        |""".stripMargin).tokens.asScala.toList

    val suggester = new ConnectSuggester(context, statement, TokenPos(5, TokenPosType.CURRENT, 4))
    println(suggester.suggest())
    println(suggester.tokens.toList)
  }

  test("connect jdbc ") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jdbc 
        |""".stripMargin).tokens.asScala.toList

    val suggester = new ConnectSuggester(context, statement, TokenPos(1, TokenPosType.NEXT, 0))
    println(suggester.suggest())
    println(suggester.tokens.toList)
  }

  test("connect jdbc where driver=\"com.mysql.jdbc.Driver\" and url=\"\"") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jdbc where driver="com.mysql.jdbc.Driver" and url=""
        |""".stripMargin).tokens.asScala.toList

    val suggester = new ConnectSuggester(context, statement, TokenPos(9, TokenPosType.CURRENT, 1))
    println(suggester.suggest())
    println(suggester.tokens.toList)
  }

  test("connect jdbc where driver=\"${driver}\"") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        |connect jdbc where driver=${dri}
        |""".stripMargin).tokens.asScala.toList

    val suggester = new ConnectSuggester(context, statement, TokenPos(5, TokenPosType.CURRENT, 5))
    println(suggester.suggest())
    println(suggester.tokens.toList)
  }
}

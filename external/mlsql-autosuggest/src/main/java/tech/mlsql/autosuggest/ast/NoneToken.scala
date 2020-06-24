package tech.mlsql.autosuggest.ast

import org.antlr.v4.runtime.{CharStream, Token, TokenSource}

/**
 * 24/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class NoneToken(token: Token) extends Token {
  override def getText: String = NoneToken.TEXT

  override def getType: Int = NoneToken.TYPE

  override def getLine: Int = token.getLine

  override def getCharPositionInLine: Int = token.getCharPositionInLine

  override def getChannel: Int = token.getChannel

  override def getTokenIndex: Int = token.getTokenIndex

  override def getStartIndex: Int = token.getStartIndex

  override def getStopIndex: Int = token.getStopIndex

  override def getTokenSource: TokenSource = token.getTokenSource

  override def getInputStream: CharStream = token.getInputStream
}

object NoneToken {
  val TEXT = "__NONE__"
  val TYPE = -3
}

package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.{TokenPos, TokenPosType}

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object LexerUtils {



  // line index should start with 1
  // column index also should start with 1
  // token index from 0
  def toTokenPos(tokens: List[Token], lineNum: Int, colNum: Int): TokenPos = {
    /**
     * load hi[cursor]...   in token
     * load [cursor]        out token
     * load[cursor]         in token
     */
    val oneLineTokens = tokens.zipWithIndex.filter { case (token, index) =>
      token.getLine == lineNum
    }
    val firstToken = oneLineTokens.head
    val lastToken = oneLineTokens.last

    if (colNum < firstToken._1.getCharPositionInLine) {
      return TokenPos(firstToken._2 - 1, TokenPosType.NEXT, 0)
    }

    if (colNum > lastToken._1.getCharPositionInLine + lastToken._1.getText.size) {
      return TokenPos(lastToken._2, TokenPosType.NEXT, 0)
    }

    if (colNum > lastToken._1.getCharPositionInLine && colNum <= lastToken._1.getCharPositionInLine + lastToken._1.getText.size) {
      return TokenPos(lastToken._2, TokenPosType.CURRENT, colNum - lastToken._1.getCharPositionInLine)
    }
    oneLineTokens.map { case (token, index) =>
      val start = token.getCharPositionInLine
      val end = token.getCharPositionInLine + token.getText.size

      if (start < colNum && colNum <= end) {
        // in token
        TokenPos(index, TokenPosType.CURRENT, colNum - start)
      } else if (colNum <= start) {
        TokenPos(index - 1, TokenPosType.NEXT, 0)
      } else {
        TokenPos(-2, -2, -2)
      }


    }.filterNot(_.pos == -2).head
  }

  def isInWhereContext(tokens: List[Token], tokenPos: Int): Boolean = {
    if (tokenPos < 1) return false
    var wherePos = -1
    (1 until tokenPos).foreach { index =>
      if (tokens(index).getType == DSLSQLLexer.WHERE || tokens(index).getType == DSLSQLLexer.OPTIONS) {
        wherePos = index
      }
    }
    if (wherePos != -1) {
      if (wherePos == tokenPos || wherePos == tokenPos - 1) return true
      val noEnd = (wherePos until tokenPos).filter(index =>
        tokens(index).getType != DSLSQLLexer.AS && tokens(index).getType != DSLSQLLexer.PARTITIONBY).isEmpty
      if (noEnd) return true

    }

    return false
  }

  def isWhereKey(tokens: List[Token], tokenPos: Int): Boolean = {
    LexerUtils.isInWhereContext(tokens, tokenPos) && (tokens(tokenPos).getText == "and" || tokens(tokenPos - 1).getText == "and")

  }
}

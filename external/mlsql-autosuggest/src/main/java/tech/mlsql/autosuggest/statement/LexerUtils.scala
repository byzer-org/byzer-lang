package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.misc.Interval
import org.apache.commons.lang3.StringUtils
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{DSLWrapper, MLSQLTokenTypeWrapper, TokenTypeWrapper}
import tech.mlsql.autosuggest.{AutoSuggestContext, TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object LexerUtils {

  def toRawSQLTokens(autoSuggestContext: AutoSuggestContext, wow: List[Token]): List[Token] = {
    val originalText = toRawSQLStr(autoSuggestContext, wow)
    val newTokens = autoSuggestContext.rawSQLLexer.tokenizeNonDefaultChannel(originalText).tokens.asScala.toList
    return newTokens
  }

  def toRawSQLStr(autoSuggestContext: AutoSuggestContext, wow: List[Token]): String = {
    val start = wow.head.getStartIndex
    val stop = wow.last.getStopIndex

    val input = wow.head.getTokenSource.asInstanceOf[DSLSQLLexer]._input
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)
    originalText
  }

  def filterPrefixIfNeeded(candidates: List[SuggestItem], tokens: List[Token], tokenPos: TokenPos) = {
    if (tokenPos.offsetInToken != 0) {
      candidates.filter(s => s.name.startsWith(tokens(tokenPos.pos).getText.substring(0, tokenPos.offsetInToken)))
    } else candidates
  }

  def filterPrefixIfNeededWithStart(candidates: List[SuggestItem], tokens: List[Token], tokenPos: TokenPos, start: Int) = {
    if (tokenPos.offsetInToken != 0) {
      candidates.filter(s => s.name.startsWith(tokens(tokenPos.pos).getText.substring(start, tokenPos.offsetInToken)))
    } else candidates
  }

  def tableTokenPrefix(tokens: List[Token], tokenPos: TokenPos): String = {
    var temp = tokens(tokenPos.pos).getText.substring(0, tokenPos.offsetInToken)
    if (tokenPos.pos > 1 && tokens(tokenPos.pos - 1).getType == TokenTypeWrapper.DOT) {
      temp = tokens(tokenPos.pos - 2).getText + "." + temp
    }
    temp
  }


  /**
   * In this method, three states of the cursor are judged in the code.
   * The three states are: first, the cursor is at the end of the code,
   * and the current line is empty, and there is no code statement under the cursor;
   * second, the cursor is in the middle of the code, and the current line is empty,
   * and there are code statements above and below the cursor; third Type,
   * the cursor is in the code statement line, and this line is not empty.
   *
   * @param tokens
   * @param lineNum 行号，从1开始计数
   * @param colNum  列号，从1开始计数
   * @return TokenPos 中的pos则是从0开始计数
   */
  def toTokenPos(tokens: List[Token], lineNum: Int, colNum: Int): TokenPos = {
    /**
     * load hi[cursor]...   in token
     * load [cursor]        out token
     * load[cursor]         in token
     */

    if (tokens.isEmpty) {
      return TokenPos(-1, TokenPosType.NEXT, -1)
    }
    // Whether to enter the flag value of the last word on the line above the record cursor
    var notEndCodeFlag: Boolean = false
    var _lastToken: Token = tokens.last
    // Determine if there is code after the line where the cursor is located
    if (_lastToken.getLine > lineNum) {
      notEndCodeFlag = true
    }
    var _lastTokenIndex = 0
    val oneLineTokens = tokens.zipWithIndex.filter { case (token, index) =>
      //A block of code that records the last word of the line before the cursor
      if (token.getLine < lineNum && notEndCodeFlag) {
        _lastToken = token
        _lastTokenIndex = index
      }
      if (!notEndCodeFlag) {
        _lastTokenIndex = index
      }
      token.getLine == lineNum
    }
    val lastToken = oneLineTokens.lastOption match {
      case Some(last) => last
      case None => (_lastToken, _lastTokenIndex)
    }
    if (oneLineTokens.isEmpty && lastToken._1.getType == DSLWrapper.SEMICOLON) {
      return TokenPos(-1, TokenPosType.NEXT, -1)
    }
    val firstToken = oneLineTokens.headOption match {
      case Some(head) => head
      case None => (_lastToken, _lastTokenIndex)
    }
    if (colNum < firstToken._1.getCharPositionInLine) {
      return TokenPos(firstToken._2 - 1, TokenPosType.NEXT, 0)
    }

    if (colNum > lastToken._1.getCharPositionInLine + lastToken._1.getText.size) {
      return TokenPos(lastToken._2, TokenPosType.NEXT, 0)
    }

    if (colNum >= lastToken._1.getCharPositionInLine
      && colNum <= lastToken._1.getCharPositionInLine + lastToken._1.getText.size
      &&
      (lastToken._1.getType != DSLSQLLexer.UNRECOGNIZED
        && lastToken._1.getType != MLSQLTokenTypeWrapper.DOT)
    ) {
      return TokenPos(lastToken._2, TokenPosType.CURRENT, colNum - lastToken._1.getCharPositionInLine)
    }
    val poses = oneLineTokens.map { case (token, index) =>
      val start = token.getCharPositionInLine
      val end = token.getCharPositionInLine + token.getText.size
      /* Immediately after a "token", there is no "space". Generally, it is regarded as a part of the previous "token",
      and the user has not finished writing it.But if this "token" is [(,).] etc, it doesn't count*/
      if (colNum == end && (1 <= token.getType)
        && (
        token.getType == DSLSQLLexer.UNRECOGNIZED
          || token.getType == MLSQLTokenTypeWrapper.DOT
        )) {
        TokenPos(index, TokenPosType.NEXT, 0)
      } else if (start < colNum && colNum <= end) {
        // in token
        TokenPos(index, TokenPosType.CURRENT, colNum - start)
      } else if (colNum <= start) {
        TokenPos(index - 1, TokenPosType.NEXT, 0)
      } else {
        TokenPos(-2, -2, -2)
      }


    }.filterNot(_.pos == -2)
    // If the result after the filter is empty, get the head directly to get the NPE
    if (poses.isEmpty) {
      return TokenPos(-1, TokenPosType.NEXT, -1)
    }
    poses.head
  }

  /**
   * Consistent with [[tech.mlsql.autosuggest.statement.LexerUtils.toTokenPos()]]
   */
  def toTokenPosForSparkSQL(tokens: List[Token], lineNum: Int, colNum: Int): TokenPos = {
    /**
     * load hi[cursor]...   in token
     * load [cursor]        out token
     * load[cursor]         in token
     */

    if (tokens.isEmpty) {
      return TokenPos(-1, TokenPosType.NEXT, -1)
    }
    // Whether to enter the flag value of the last word on the line above the record cursor
    var notEndCodeFlag: Boolean = false
    var _lastToken: Token = tokens.last
    // Determine if there is code after the line where the cursor is located
    if (_lastToken.getLine > lineNum) {
      notEndCodeFlag = true
    }
    var _lastTokenIndex = 0
    val oneLineTokens = tokens.zipWithIndex.filter { case (token, index) =>
      /* A block of code that records the last word of the line before the cursor */
      if (token.getLine < lineNum && notEndCodeFlag) {
        _lastToken = token
        _lastTokenIndex = index
      }
      if (!notEndCodeFlag) {
        _lastTokenIndex = index
      }
      token.getLine == lineNum
    }
    val lastToken = oneLineTokens.lastOption match {
      case Some(last) => last
      case None => (_lastToken, _lastTokenIndex)
    }
    if (oneLineTokens.isEmpty && lastToken._1.getType == DSLWrapper.SEMICOLON) {
      return TokenPos(-1, TokenPosType.NEXT, -1)
    }
    val firstToken = oneLineTokens.headOption match {
      case Some(head) => head
      case None => (_lastToken, _lastTokenIndex)
    }
    if (colNum < firstToken._1.getCharPositionInLine) {
      return TokenPos(firstToken._2 - 1, TokenPosType.NEXT, 0)
    }

    if (colNum > lastToken._1.getCharPositionInLine + lastToken._1.getText.size) {
      return TokenPos(lastToken._2, TokenPosType.NEXT, 0)
    }

    if (colNum >= lastToken._1.getCharPositionInLine
      && colNum <= lastToken._1.getCharPositionInLine + lastToken._1.getText.size
      && !TokenTypeWrapper.MAP.contains(lastToken._1.getType)

    ) {
      return TokenPos(lastToken._2, TokenPosType.CURRENT, colNum - lastToken._1.getCharPositionInLine)
    }
    val poses = oneLineTokens.map { case (token, index) =>
      val start = token.getCharPositionInLine
      val end = token.getCharPositionInLine + token.getText.size
      /* Immediately after a "token", there is no "space". Generally, it is regarded as a part of the previous "token",
      and the user has not finished writing it.But if this "token" is [(,).] etc, it doesn't count*/
      if (colNum == end && (1 <= token.getType)
        && (
        TokenTypeWrapper.MAP.contains(token.getType)
        )) {
        TokenPos(index, TokenPosType.NEXT, 0)
      } else if (start < colNum && colNum <= end) {
        // in token
        TokenPos(index, TokenPosType.CURRENT, colNum - start)
      } else if (colNum <= start) {
        TokenPos(index - 1, TokenPosType.NEXT, 0)
      } else {
        TokenPos(-2, -2, -2)
      }


    }.filterNot(_.pos == -2)
    // If the result after the filter is empty, get the head directly to get the NPE
    if (poses.isEmpty) {
      return TokenPos(-1, TokenPosType.NEXT, -1)
    }
    poses.head
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

  def cleanStr(str: String): String = {
    if (StringUtils.isEmpty(str)) {
      return str
    }
    if (str.startsWith("`") || str.startsWith("\"") || (str.startsWith("'") && !str.startsWith("'''")))
      str.substring(1, str.length - 1)
    else if (str.startsWith("'''")) {
      str.substring(3, str.length - 3)
    } else str
  }

  def cleanStrReturnStrAndSep(str: String): (String, String) = {
    if (StringUtils.isEmpty(str)) {
      return (str, "")
    }
    if (str.startsWith("`")) {
      (str.substring(1, str.length - 1), "`")
    } else if (str.startsWith("\"")) {
      (str.substring(1, str.length - 1), "\"")
    } else if (str.startsWith("'") && !str.startsWith("'''")) {
      (str.substring(1, str.length - 1), "'")
    } else if (str.startsWith("'''")) {
      (str.substring(3, str.length - 3), "'''")
    } else (str, "")
  }


  def cleanTokenPrefix(partialToken: String, suggestItems: List[SuggestItem]): List[SuggestItem] = {

    val text = LexerUtils.cleanStr(partialToken)
    suggestItems.map { item =>
      var res: SuggestItem = null
      val (itemName, sep) = LexerUtils.cleanStrReturnStrAndSep(item.name)
      if (StringUtils.isBlank(text)) {
        res = SuggestItem(itemName, item.metaTable, Map())
      } else if (itemName.equals(text)) {
        res = null
      } else if (itemName.contains(text)) {
        res = SuggestItem(itemName, item.metaTable, Map())
      } else if (StringUtils.isBlank(text.split("\\s+").toList.head)) {
        res = SuggestItem(itemName + sep, item.metaTable, Map())
      } else if (itemName.contains(text.split("\\s+").toList.head)) {
        res = SuggestItem(itemName + sep, item.metaTable, Map())
      }
      res
    }.filter(_ != null)
  }
}


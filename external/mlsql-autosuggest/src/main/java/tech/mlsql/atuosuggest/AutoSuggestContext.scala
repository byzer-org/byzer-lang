package tech.mlsql.atuosuggest

import com.intigua.antlr4.autosuggest.LexerWrapper
import org.antlr.v4.runtime.Token
import org.apache.spark.sql.SparkSession
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.statement.LoadSuggester

import scala.collection.mutable.ArrayBuffer

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AutoSuggestContext(val session: SparkSession, val lexer: LexerWrapper) {
  val statements = ArrayBuffer[List[Token]]()

  def build(_tokens: List[Token]): AutoSuggestContext = {
    val tokens = _tokens.zipWithIndex
    var start = 0
    var end = 0
    tokens.foreach { case (token, index) =>
      // statement end
      if (token.getType == DSLSQLLexer.T__1) {
        end = index
        statements.append(tokens.filter(p => p._2 >= start && p._2 <= end).map(_._1))
        start = index + 1
      }

    }
    // clean the last statement without ender
    val theLeft = tokens.filter(p => p._2 >= start && p._2 <= tokens.size).map(_._1).toList
    if (theLeft.size > 0) {
      statements.append(theLeft)
    }
    return this
  }

  def toRelativePos(tokenPos: TokenPos): (TokenPos, Int) = {
    var skipSize = 0
    var targetIndex = 0
    var targetPos: TokenPos = null
    var targetStaIndex = 0
    statements.zipWithIndex.foreach { case (sta, index) =>
      val relativePos = tokenPos.pos - skipSize
      if (relativePos >= 0 && relativePos < sta.size) {
        targetPos = tokenPos.copy(pos = tokenPos.pos - skipSize)
        targetStaIndex = index
      }
      skipSize += sta.size
      targetIndex += 1
    }
    return (targetPos, targetStaIndex)
  }

  /**
   * Notice that the pos in tokenPos is in whole script.
   * We need to convert it to the relative pos in every statement
   */
  def suggest(tokenPos: TokenPos) = {

    val (relativeTokenPos, index) = toRelativePos(tokenPos)
    statements(index).headOption.map(_.getText) match {
      case Some("load") =>
        val suggester = new LoadSuggester(this, statements(index), relativeTokenPos)
        suggester.suggest()
      case Some(value) => firstWords.filter(_.startsWith(value))
      case None => firstWords
    }
  }

  private val firstWords = List("load", "select", "include", "register", "run", "train", "save", "set")


}

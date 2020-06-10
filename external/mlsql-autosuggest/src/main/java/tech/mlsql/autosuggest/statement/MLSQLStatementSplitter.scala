package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer

import scala.collection.mutable.ArrayBuffer

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLStatementSplitter extends StatementSplitter {
  override def split(_tokens: List[Token]): List[List[Token]] = {
    val _statements = ArrayBuffer[List[Token]]()
    val tokens = _tokens.zipWithIndex
    var start = 0
    var end = 0
    tokens.foreach { case (token, index) =>
      // statement end
      if (token.getType == DSLSQLLexer.T__1) {
        end = index
        _statements.append(tokens.filter(p => p._2 >= start && p._2 <= end).map(_._1))
        start = index + 1
      }

    }
    // clean the last statement without ender
    val theLeft = tokens.filter(p => p._2 >= start && p._2 <= tokens.size).map(_._1).toList
    if (theLeft.size > 0) {
      _statements.append(theLeft)
    }
    _statements.toList
  }
}

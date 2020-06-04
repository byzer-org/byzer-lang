package tech.mlsql.atuosuggest

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.atuosuggest.dsl.{Food, TokenMatcher}
import tech.mlsql.atuosuggest.statement.MatchAndExtractor

import scala.collection.mutable.ArrayBuffer

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AttributeExtractor(tokens: List[Token]) extends MatchAndExtractor[String] {
  override def matcher(start: Int): TokenMatcher = {
    val temp = TokenMatcher(tokens, start).
      eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, SqlBaseLexer.T__3)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).
      eat(Food(None, SqlBaseLexer.AS)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
      build
    temp
  }

  override def extractor(start: Int, end: Int): String = {
    val attrTokens = tokens.slice(start, end)
    val token = attrTokens.last
    if (token.getType == SqlBaseLexer.ASTERISK) {
      return attrTokens.map(_.getText).mkString("")
    }
    token.getText
  }

  override def iterate(start: Int, end: Int, limit: Int): List[String] = {
    val attributes = ArrayBuffer[String]()
    var matchRes = matcher(start)
    var whileLimit = 1000
    while (matchRes.isSuccess && whileLimit > 0) {
      attributes += extractor(matchRes.start, matchRes.get)
      whileLimit -= 1
      val temp = TokenMatcher(tokens, matchRes.get).eat(Food(None, SqlBaseLexer.T__2)).build
      if (temp.isSuccess) {
        matchRes = matcher(temp.get)
      } else whileLimit = 0
    }
    attributes.toList
  }
}

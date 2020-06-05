package tech.mlsql.atuosuggest

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.atuosuggest.dsl.{Food, TokenMatcher}
import tech.mlsql.atuosuggest.statement.MatchAndExtractor

import scala.collection.mutable.ArrayBuffer

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AttributeExtractor(autoSuggestContext: AutoSuggestContext, tokens: List[Token]) extends MatchAndExtractor[String] {
  override def matcher(start: Int): TokenMatcher = {
    return funcitonM(start)
  }

  private def attributeM(start: Int): TokenMatcher = {
    val temp = TokenMatcher(tokens, start).
      eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, SqlBaseLexer.T__3)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).
      eat(Food(None, SqlBaseLexer.AS)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
      build
    temp
  }

  private def funcitonM(start: Int): TokenMatcher = {
    // deal with something like: sum(a[1],fun(b)) as a
    val temp = TokenMatcher(tokens, start).eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, SqlBaseLexer.T__0)).build
    // function match
    if (temp.isSuccess) {
      // try to find AS
      // we need to take care of situation like this: cast(a as int) as b
      // In future, we should get first function and get the return type so we can get the b type.
      val index = TokenMatcher(tokens, start).index(Array(Food(None, SqlBaseLexer.T__1), Food(None, SqlBaseLexer.AS), Food(None, SqlBaseLexer.IDENTIFIER)))
      if (index != -1) {
        //index + 1 to skip )
        val aliasName = TokenMatcher(tokens, index+1).eat(Food(None, SqlBaseLexer.AS)).
          eat(Food(None, SqlBaseLexer.IDENTIFIER)).build
        if (aliasName.isSuccess) {
          return TokenMatcher.resultMatcher(tokens, start, aliasName.get)
        }

      }
      // if no AS, do nothing
      null
    }
    return attributeM(start)
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

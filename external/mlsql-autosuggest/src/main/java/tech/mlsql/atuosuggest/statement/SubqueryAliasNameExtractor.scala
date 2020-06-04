package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import tech.mlsql.atuosuggest.dsl.TokenMatcher

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SubqueryAliasNameExtractor(tokens: List[Token]) extends MatchAndExtractor[String] {
  override def matcher(start: Int): TokenMatcher = {

  }

  override def extractor(start: Int, end: Int): String = ???

  override def iterate(start: Int, end: Int, limit: Int): List[String] = ???
}



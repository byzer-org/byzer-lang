package tech.mlsql.atuosuggest.statement

import tech.mlsql.atuosuggest.dsl.TokenMatcher

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait MatchAndExtractor[T] {
  def matcher(start: Int): TokenMatcher

  def extractor(start: Int, end: Int): T

  def iterate(start: Int, end: Int, limit: Int = 100): List[T]
}

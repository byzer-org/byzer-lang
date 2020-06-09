package tech.mlsql.autosuggest.statement

import tech.mlsql.autosuggest.dsl.TokenMatcher

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait MatchAndExtractor[T] {
  def matcher(start: Int): TokenMatcher

  def extractor(start: Int, end: Int): List[T]

  def iterate(start: Int, end: Int, limit: Int = 100): List[T]
}

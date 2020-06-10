package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait StatementSplitter {
  def split(_tokens: List[Token]): List[List[Token]]
}

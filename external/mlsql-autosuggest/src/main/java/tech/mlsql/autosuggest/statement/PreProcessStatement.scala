package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait PreProcessStatement {
  def process(statement: List[Token]): Unit
}

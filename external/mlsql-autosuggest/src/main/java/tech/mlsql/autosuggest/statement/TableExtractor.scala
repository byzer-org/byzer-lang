package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.autosuggest.AutoSuggestContext
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher, TokenTypeWrapper}
import tech.mlsql.autosuggest.meta.MetaTableKey

import scala.collection.mutable.ArrayBuffer

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TableExtractor(autoSuggestContext: AutoSuggestContext, ast: SingleStatementAST, tokens: List[Token]) extends MatchAndExtractor[MetaTableKeyWrapper] {
  override def matcher(start: Int): TokenMatcher = {
    val temp = TokenMatcher(tokens, start).
      eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, TokenTypeWrapper.DOT)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).
      eat(Food(None, SqlBaseLexer.AS)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
      build
    temp
  }

  override def extractor(start: Int, end: Int): List[MetaTableKeyWrapper] = {
    val dbTableTokens = tokens.slice(start, end)
    val dbTable = dbTableTokens.length match {
      case 2 =>
        val List(tableToken, aliasToken) = dbTableTokens
        MetaTableKeyWrapper(MetaTableKey(None, None, tableToken.getText), Option(aliasToken.getText))
      case 3 =>
        val List(dbToken, _, tableToken) = dbTableTokens
        MetaTableKeyWrapper(MetaTableKey(None, Option(dbToken.getText), tableToken.getText), None)
      case 4 =>
        val List(dbToken, _, tableToken, aliasToken) = dbTableTokens
        MetaTableKeyWrapper(MetaTableKey(None, Option(dbToken.getText), tableToken.getText), Option(aliasToken.getText))
      case 5 =>
        val List(dbToken, _, tableToken, _, aliasToken) = dbTableTokens
        MetaTableKeyWrapper(MetaTableKey(None, Option(dbToken.getText), tableToken.getText), Option(aliasToken.getText))
      case _ => MetaTableKeyWrapper(MetaTableKey(None, None, dbTableTokens.head.getText), None)
    }

    List(dbTable)
  }

  override def iterate(start: Int, end: Int, limit: Int = 100): List[MetaTableKeyWrapper] = {
    val tables = ArrayBuffer[MetaTableKeyWrapper]()
    var matchRes = matcher(start)
    var whileLimit = limit
    while (matchRes.isSuccess && whileLimit > 0) {
      tables ++= extractor(matchRes.start, matchRes.get)
      whileLimit -= 1
      val temp = TokenMatcher(tokens, matchRes.get).eat(Food(None, SqlBaseLexer.T__2)).build
      if (temp.isSuccess) {
        matchRes = matcher(temp.get)
      } else whileLimit = 0
    }

    tables.toList
  }
}

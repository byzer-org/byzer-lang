package tech.mlsql.autosuggest

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher}
import tech.mlsql.autosuggest.meta.MetaTableKey
import tech.mlsql.autosuggest.statement.{MatchAndExtractor, MetaTableKeyWrapper, SingleStatementAST}

import scala.collection.mutable.ArrayBuffer

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AttributeExtractor(autoSuggestContext: AutoSuggestContext, ast: SingleStatementAST, tokens: List[Token]) extends MatchAndExtractor[String] {

  override def matcher(start: Int): TokenMatcher = {
    return asterriskM(start)
  }

  private def attributeM(start: Int): TokenMatcher = {
    val temp = TokenMatcher(tokens, start).
      eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, SqlBaseLexer.T__3)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).
      eat(Food(None, SqlBaseLexer.AS)).optional.
      eat(Food(None, SqlBaseLexer.IDENTIFIER)).optional.
      build
    temp.isSuccess match {
      case true => temp
      case false =>
        TokenMatcher(tokens, start).
          eatOneAny.
          eat(Food(None, SqlBaseLexer.AS)).
          eat(Food(None, SqlBaseLexer.IDENTIFIER)).
          build
    }
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
        val aliasName = TokenMatcher(tokens, index + 1).eat(Food(None, SqlBaseLexer.AS)).
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

  private def asterriskM(start: Int): TokenMatcher = {
    val temp = TokenMatcher(tokens, start).
      eat(Food(None, SqlBaseLexer.IDENTIFIER), Food(None, SqlBaseLexer.T__3)).optional.
      eat(Food(None, SqlBaseLexer.ASTERISK)).build
    if (temp.isSuccess) {
      return TokenMatcher.resultMatcher(tokens, start, temp.get)
    }
    return funcitonM(start)
  }

  override def extractor(start: Int, end: Int): List[String] = {


    val attrTokens = tokens.slice(start, end)
    val token = attrTokens.last
    if (token.getType == SqlBaseLexer.ASTERISK) {
      return attrTokens match {
        case List(tableName, _, _) =>
          //expand output
          ast.selectSuggester.table_info(ast.level).
            get(MetaTableKeyWrapper(MetaTableKey(None, None, null), Option(tableName.getText))).orElse{
            ast.selectSuggester.table_info(ast.level).
              get(MetaTableKeyWrapper(MetaTableKey(None, None, tableName.getText), None))
          } match {
            case Some(table) =>
              //如果是临时表，那么需要进一步展开
              val columns = if (table.key.db == Option(SpecialTableConst.TEMP_TABLE_DB_KEY)) {
                autoSuggestContext.metaProvider.search(table.key) match {
                  case Some(item) => item.columns
                  case None => List()
                }
              } else table.columns
              columns.map(_.name).toList
            case None => List()
          }
        case List(starAttr) =>
          val table = ast.tables(tokens).head
          ast.selectSuggester.table_info(ast.level).
            get(table) match {
            case Some(table) =>
              //如果是临时表，那么需要进一步展开
              val columns = if (table.key.db == Option(SpecialTableConst.TEMP_TABLE_DB_KEY)) {
                autoSuggestContext.metaProvider.search(table.key) match {
                  case Some(item) => item.columns
                  case None => List()
                }
              } else table.columns
              columns.map(_.name).toList
            case None => List()
          }
      }
    }
    List(token.getText)
  }

  override def iterate(start: Int, end: Int, limit: Int): List[String] = {
    val attributes = ArrayBuffer[String]()
    var matchRes = matcher(start)
    var whileLimit = 1000
    while (matchRes.isSuccess && whileLimit > 0) {
      attributes ++= extractor(matchRes.start, matchRes.get)
      whileLimit -= 1
      val temp = TokenMatcher(tokens, matchRes.get).eat(Food(None, SqlBaseLexer.T__2)).build
      if (temp.isSuccess) {
        matchRes = matcher(temp.get)
      } else whileLimit = 0
    }
    attributes.toList
  }
}

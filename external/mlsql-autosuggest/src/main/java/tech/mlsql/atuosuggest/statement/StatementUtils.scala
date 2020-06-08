package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.atuosuggest.dsl.{Food, TokenMatcher, TokenTypeWrapper}
import tech.mlsql.atuosuggest.{TokenPos, TokenPosType}

/**
 * 8/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait StatementUtils {
  def selectSuggester: SelectSuggester

  def tokenPos: TokenPos

  def tokens: List[Token]


  def level = {
    var targetLevel = 0
    selectSuggester.sqlAST.visitDown(0) { case (ast, _level) =>
      if (tokenPos.pos >= ast.start && tokenPos.pos < ast.stop) targetLevel = _level
    }
    targetLevel
  }

  def table_info = {
    val _level = if (
      (selectSuggester.table_info.size == 1 && level == 0) ||
        level == selectSuggester.table_info.size - 1
    ) 0 else level + 1
    selectSuggester.table_info.get(_level)
  }


  def tableSuggest(): List[SuggestItem] = {
    table_info match {
      case Some(tb) => tb.keySet.map { key =>
        key.aliasName.getOrElse(key.metaTableKey.table)
      }.map(SuggestItem(_)).toList
      case None => List()
    }
  }

  def attributeSuggest(): List[SuggestItem] = {
    val tempStart = tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        tokenPos.pos - 1
      case TokenPosType.NEXT =>
        tokenPos.pos
    }

    def allOutput = {
      table_info.get.flatMap { case (_, metaTable) =>
        metaTable.columns.map(column => SuggestItem(column.name)).toList
      }.toList
    }

    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
    if (temp.isSuccess) {
      val table = temp.getMatchTokens.head.getText
      table_info.get.filter { case (key, value) =>
        key.aliasName.isDefined && key.aliasName.get == table
      }.headOption match {
        case Some(table) => table._2.columns.map(column => SuggestItem(column.name)).toList
        case None => allOutput
      }
    } else allOutput

  }

  def functionSuggest(): List[SuggestItem] = {
    List()
  }
}

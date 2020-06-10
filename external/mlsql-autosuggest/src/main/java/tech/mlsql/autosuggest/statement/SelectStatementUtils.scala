package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher, TokenTypeWrapper}
import tech.mlsql.autosuggest.{MLSQLSQLFunction, TokenPos, TokenPosType}

/**
 * 8/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait SelectStatementUtils {
  def selectSuggester: SelectSuggester

  def tokenPos: TokenPos

  def tokens: List[Token]


  def levelFromTokenPos = {
    var targetLevel = 0
    selectSuggester.sqlAST.visitDown(0) { case (ast, _level) =>
      if (tokenPos.pos >= ast.start && tokenPos.pos < ast.stop) targetLevel = _level
    }
    targetLevel
  }

  def getASTFromTokenPos: Option[SingleStatementAST] = {
    var targetAst: Option[SingleStatementAST] = None
    selectSuggester.sqlAST.visitUp(0) { case (ast, level) =>
      if (targetAst == None && (ast.start <= tokenPos.pos && tokenPos.pos < ast.stop)) {
        targetAst = Option(ast)
      }
    }
    targetAst
  }

  def table_info = {
    var _level = levelFromTokenPos + 1
    if (selectSuggester.table_info.size == 1 && levelFromTokenPos == 0) {
      _level = 0
    }

    if (levelFromTokenPos == selectSuggester.table_info.size - 1) {
      _level = levelFromTokenPos
    }
    selectSuggester.table_info.get(_level)
  }


  def tableSuggest(): List[SuggestItem] = {
    table_info match {
      case Some(tb) => tb.map { case (key, value) =>
        (key.aliasName.getOrElse(key.metaTableKey.table), value)
      }.map { case (name, table) =>
        SuggestItem(name, table, Map())
      }.toList
      case None => selectSuggester.context.metaProvider.list.map { item =>
        SuggestItem(item.key.table, item, Map())
      }
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
        metaTable.columns.map(column => SuggestItem(column.name, metaTable, Map())).toList
      }.toList
    }

    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
    if (temp.isSuccess) {
      val table = temp.getMatchTokens.head.getText
      table_info.get.filter { case (key, value) =>
        key.aliasName.isDefined && key.aliasName.get == table
      }.headOption match {
        case Some(table) => table._2.columns.map(column => SuggestItem(column.name, table._2, Map())).toList
        case None => allOutput
      }
    } else allOutput

  }

  def functionSuggest(): List[SuggestItem] = {
    def allOutput = {
      MLSQLSQLFunction.funcMetaProvider.list.map(item => SuggestItem(item.key.table, item, Map()))
    }

    val tempStart = tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        tokenPos.pos - 1
      case TokenPosType.NEXT =>
        tokenPos.pos
    }

    // 如果匹配上了，说明是字段，那么就不应该提示函数了
    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
    if (temp.isSuccess) {
      List()
    } else allOutput

  }
}

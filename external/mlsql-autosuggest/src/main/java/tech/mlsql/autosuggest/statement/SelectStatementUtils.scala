package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher, TokenTypeWrapper}
import tech.mlsql.autosuggest.{MLSQLSQLFunction, SpecialTableConst, TokenPos, TokenPosType}
import tech.mlsql.common.utils.log.Logging

/**
 * 8/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait SelectStatementUtils extends Logging {
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
    selectSuggester.table_info.get(levelFromTokenPos)
  }


  def tableSuggest(): List[SuggestItem] = {
    val tempStart = tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        tokenPos.pos - 1
      case TokenPosType.NEXT =>
        tokenPos.pos
    }

    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build

    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"tableSuggest, table_info:\n")
      logInfo("========tableSuggest start=======")
      if (table_info.isDefined) {
        table_info.get.foreach { case (key, metaTable) =>
          logInfo(key.toString + s"=>\n    key: ${metaTable.key} \n    columns: ${metaTable.columns.map(_.name).mkString(",")}")
        }
      }

      logInfo(s"Final: suggest ${!temp.isSuccess}")

      logInfo("========tableSuggest end =======")
    }

    if (temp.isSuccess) return List()


    table_info match {
      case Some(tb) => tb.map { case (key, value) =>
        (key.aliasName.getOrElse(key.metaTableKey.table), value)
      }.map { case (name, table) =>
        SuggestItem(name, table, Map())
      }.toList
      case None => selectSuggester.context.metaProvider.list(Map()).map { item =>
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
    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"attributeSuggest:\n")
      logInfo("========attributeSuggest start=======")
    }

    def allOutput = {
      /**
       * 优先推荐别名
       */
      val res = table_info.get.filter { item => item._2.key == SpecialTableConst.subQueryAliasTable && item._1.aliasName.isDefined }.flatMap { table =>
        if (selectSuggester.context.isInDebugMode) {
          val columns = table._2.columns.map { item => s"${item.name} ${item}" }.mkString("\n")
          logInfo(s"TARGET table: ${table._1} \n columns: \n[${columns}] ")
        }
        table._2.columns.map(column => SuggestItem(column.name, table._2, Map()))

      }.toList

      if (res.isEmpty) {
        if (selectSuggester.context.isInDebugMode) {
          val tables = table_info.get.map { case (key, table) =>
            val columns = table.columns.map { item => s"${item.name} ${item}" }.mkString("\n")
            s"${key}:\n ${columns}"
          }.mkString("\n")
          logInfo(s"ALL tables: \n ${tables}")
        }
        table_info.get.flatMap { case (_, metaTable) =>
          metaTable.columns.map(column => SuggestItem(column.name, metaTable, Map())).toList
        }.toList
      } else res


    }

    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"Try to match attribute db prefix: Status(${temp.isSuccess})")
    }
    val res = if (temp.isSuccess) {
      val table = temp.getMatchTokens.head.getText
      table_info.get.filter { case (key, value) =>
        (key.aliasName.isDefined && key.aliasName.get == table) || key.metaTableKey.table == table
      }.headOption match {
        case Some(table) =>
          if (selectSuggester.context.isInDebugMode) {
            logInfo(s"table[${table._1}] found, return ${table._2.key} columns.")
          }
          table._2.columns.map(column => SuggestItem(column.name, table._2, Map())).toList
        case None =>
          if (selectSuggester.context.isInDebugMode) {
            logInfo(s"No table found, so return all table[${table_info.get.map { case (_, metaTable) => metaTable.key.toString }}] columns.")
          }
          allOutput
      }
    } else allOutput

    if (selectSuggester.context.isInDebugMode) {
      logInfo("========attributeSuggest end=======")
    }
    res

  }

  def functionSuggest(): List[SuggestItem] = {
    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"functionSuggest:\n")
      logInfo("========functionSuggest start=======")
    }

    def allOutput = {
      MLSQLSQLFunction.funcMetaProvider.list(Map()).map(item => SuggestItem(item.key.table, item, Map()))
    }

    val tempStart = tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        tokenPos.pos - 1
      case TokenPosType.NEXT =>
        tokenPos.pos
    }

    // 如果匹配上了，说明是字段，那么就不应该提示函数了
    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
    val res = if (temp.isSuccess) {
      List()
    } else allOutput

    if (selectSuggester.context.isInDebugMode) {
      logInfo(s"functions: ${allOutput.map(_.name).mkString(",")}")
      logInfo("========functionSuggest end=======")
    }
    res

  }
}

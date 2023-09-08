package tech.mlsql.autosuggest.preprocess

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.SpecialTableConst.{ET_TABLE, TEMP_TABLE_DB_KEY}
import tech.mlsql.autosuggest.dsl.{DSLWrapper, Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.autosuggest.statement.{LexerUtils, PreProcessStatement, SelectSuggester}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TablePreprocessor(context: AutoSuggestContext) extends PreProcessStatement {

  /**
   *
   * load 语句和select语句比较特殊
   * Load语句要获取 真实表
   * select 语句要获取最后的select 语句
   *
   * load语句获取真实表的时候会加一个prefix前缀，该值等于load语句里的format
   */
  def process(statement: List[Token]): Unit = {
    val tempTableProvider = context.tempTableProvider
    val tempMatcher = TokenMatcher(statement, statement.size - 2).back.eat(Food(None, DSLSQLLexer.IDENTIFIER), Food(None, DSLSQLLexer.AS)).build

    if (tempMatcher.isSuccess) {
      val tableName = tempMatcher.getMatchTokens.last.getText
      val defaultTable = SpecialTableConst.tempTable(tableName)
      val table = statement(0).getText.toLowerCase match {
        case "load" =>
          // This code will cause huge performance issue since we should
          // infer the table schema from the data source.
          // The user should use execute the load statement first, and then the meta will
          // store in session catalog and we can get the meta in the next time.
          //          val formatMatcher = TokenMatcher(statement, 1).
          //            eat(Food(None, DSLSQLLexer.IDENTIFIER),
          //              Food(None, MLSQLTokenTypeWrapper.DOT),
          //              Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).build
          //          if (formatMatcher.isSuccess) {
          //
          //            formatMatcher.getMatchTokens.map(_.getText) match {
          //              case List(format, _, path) =>
          //                LexerUtils.cleanStr(path).split("\\.", 2) match {
          //                  case Array(db, table) =>
          //                    context.metaProvider.search(MetaTableKey(Option(format), Option(db), table)).getOrElse(defaultTable)
          //                  case Array(table) =>
          //                    context.metaProvider.search(MetaTableKey(Option(format), None, table)).getOrElse(defaultTable)
          //                }
          //            }
          //          } else {
          //            defaultTable
          //          }
          defaultTable
        case "select" =>
          //statement.size - 3 是为了移除 最后的as table语句
          val selectSuggester = new SelectSuggester(context, statement.slice(0, statement.size - 3), TokenPos(0, TokenPosType.NEXT, -1))
          val columns = selectSuggester.sqlAST.output(selectSuggester.tokens).map { name =>
            MetaTableColumn(name, null, true, Map())
          }
          MetaTable(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), tableName), columns)
        case _ => defaultTable
      }

      tempTableProvider.register(tableName, table)
    }
    statement(0).getText.toLowerCase match {
      case "train" | "run" =>
        val tempTokenMatcher = TokenMatcher(statement, 1).forward
          .eat(Food(None, DSLSQLLexer.IDENTIFIER), Food(None, DSLSQLLexer.AS))
          .eat(Food(None, DSLSQLLexer.IDENTIFIER))
          .eat(Food(None, DSLWrapper.DOT))
          .eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER))
          .build
        if (!tempTokenMatcher.isSuccess) {
          return
        }
        val pathQuote = LexerUtils.cleanStr(tempTokenMatcher.getMatchTokens.last.getText)
        val modelProvider = context.modelProvider
        var tableName: String = ET_TABLE.key.table
        if (tempMatcher.isSuccess) {
          tableName = tempMatcher.getMatchTokens.last.getText
        }
        val metaTable = MetaTable(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), tableName, pathQuote), List())
        modelProvider.register(tableName, metaTable)

      case _=>
    }
  }
}
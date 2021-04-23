package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.core.datasource.{DataSourceRegistry, MLSQLSourceInfo}
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos}

import scala.collection.mutable

class SaveSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {
  private val subSuggesters = mutable.HashMap[String, StatementSuggester]()

  register(classOf[SaveModeSuggester])
  register(classOf[SaveFormatSuggester])
  register(classOf[SaveOptionsSuggester])
  register(classOf[SavePathQuoteSuggester])

  override def register(clazz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clazz.getConstructor(classOf[SaveSuggester]).newInstance(this)
    subSuggesters += instance.name -> instance
    this
  }

  override def name: String = "save"

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.SAVE) => true
      case _ => false
    }
  }

  override def suggest(): List[SuggestItem] = {
    defaultSuggest(subSuggesters.toMap)
  }
}

/**
 * 提示存储的方式，包括overwrite、append、errorIfExists、ignore
 */
private class SaveModeSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "mode"

  override def isMatch(): Boolean = {
    firstAhead(SaveModeSuggester.SAVE_MODE_TOKENS: _*).contains(tokenPos.pos - 1)
  }

  override def suggest(): List[SuggestItem] = {
    SaveModeSuggester.SAVE_MODE_SUGGESTIONS
  }
}

private object SaveModeSuggester {
  val SAVE_MODE_TOKENS = Seq(
    DSLSQLLexer.SAVE,
    DSLSQLLexer.OVERWRITE,
    DSLSQLLexer.APPEND,
    DSLSQLLexer.ERRORIfExists,
    DSLSQLLexer.IGNORE)

  val SAVE_MODE_SUGGESTIONS = List(
    SuggestItem("overwrite", SpecialTableConst.KEY_WORD_TABLE, Map.empty),
    SuggestItem("append", SpecialTableConst.KEY_WORD_TABLE, Map.empty),
    SuggestItem("errorIfExists", SpecialTableConst.KEY_WORD_TABLE, Map.empty),
    SuggestItem("ignore", SpecialTableConst.KEY_WORD_TABLE, Map.empty))
}

/**
 * 提示存储格式
 */
private class SaveFormatSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "format"

  override def isMatch(): Boolean = {
    backAndFirstIs(DSLSQLLexer.AS)
  }

  override def suggest(): List[SuggestItem] = {
    val availableFormats = (DataSourceRegistry.allSourceNames ++ StatementUtils.SUGGEST_FORMATS).distinct
    LexerUtils.filterPrefixIfNeeded(
      availableFormats.map(SuggestItem(_, SpecialTableConst.DATA_SOURCE_TABLE, Map("desc" -> "DataSource"))).toList,
      tokens,
      tokenPos)
  }

  override def tokens: List[Token] = saveSuggester._tokens

  override def tokenPos: TokenPos = saveSuggester._tokenPos
}

/**
 * 提示存储的参数，即where或options子句的内容
 */
private class SaveOptionsSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "options"

  override def isMatch(): Boolean = {
    backAndFirstIs(DSLSQLLexer.OPTIONS) || backAndFirstIs(DSLSQLLexer.WHERE)
  }

  override def suggest(): List[SuggestItem] = {
    val asToken = firstAhead(DSLSQLLexer.AS)
    asToken match {
      case Some(pos) =>
        // 取得当前保存的类型
        val formatToken = tokens(pos + 1)

        // 取得需要提示的所有数据源名称及描述
        val dataSources = DataSourceRegistry.fetch(formatToken.getText, Map.empty) match {
          case Some(sourceInfo: MLSQLSourceInfo) =>
            sourceInfo.explainParams(saveSuggester.context.session)
              .collect()
              .map(row => (row.getString(0), row.getString(1)))
              .toList
          case None => List.empty
        }

        val suggestions = dataSources.map(tuple =>
          SuggestItem(tuple._1, SpecialTableConst.OPTION_TABLE, Map("desc" -> tuple._2)))
        LexerUtils.filterPrefixIfNeeded(suggestions, tokens, tokenPos)
      case None => List.empty
    }
  }
}

/**
 * 在输入存储类型后提示&#96;&#96;<br>由于前端在输入点号时不会发出请求，目前未生效
 */
private class SavePathQuoteSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "pathQuote"

  override def isMatch(): Boolean = {
    val matchingResult = TokenMatcher(tokens, tokenPos.pos)
      .back
      .eat(Food(None, MLSQLTokenTypeWrapper.DOT))
      .eat(Food(None, DSLSQLLexer.IDENTIFIER))
      .eat(Food(None, DSLSQLLexer.AS))
      .build
    matchingResult.isSuccess
  }

  override def suggest(): List[SuggestItem] = {
    val quoteSuggestion = SuggestItem("``", SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table"))
    LexerUtils.filterPrefixIfNeeded(List(quoteSuggestion), tokens, tokenPos)
  }
}

private abstract class SaveSuggesterBase(saveSuggester: SaveSuggester) extends StatementSuggester with StatementUtils {
  override def tokens: List[Token] = saveSuggester._tokens

  override def tokenPos: TokenPos = saveSuggester._tokenPos
}

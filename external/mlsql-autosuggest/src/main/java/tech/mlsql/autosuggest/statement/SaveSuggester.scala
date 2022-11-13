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
  register(classOf[SaveTableSuggester])
  register(classOf[SaveAsSuggester])
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
    // save overwrite tt as
    defaultSuggest(subSuggesters.toMap)
  }
}


private class SaveTableSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "table"

  override def isMatch(): Boolean = {
    val matchSize = SaveModeSuggester.SAVE_MODE_TOKENS.map { item =>
      TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, item)).isSuccess
    }.filter(item => item).length
    matchSize > 0
  }

  override def suggest(): List[SuggestItem] = {

    if (isOptionKeywordShouldPromp(Food(None, DSLSQLLexer.IDENTIFIER))) {
      return List(
        SuggestItem("as", SpecialTableConst.KEY_WORD_TABLE, Map())
      )
    }

    saveSuggester.context.metaProvider.list(Map()).map { item =>
      SuggestItem(item.key.table, SpecialTableConst.tempTable(item.key.table), Map())
    }
  }
}

private class SaveAsSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "as_format"

  override def isMatch(): Boolean = {
    val matchSize = SaveModeSuggester.SAVE_MODE_TOKENS.map { item =>
      backTwoStepIs(item)
    }.filter(item => item).length

    backOneStepIs(DSLSQLLexer.IDENTIFIER) && matchSize > 0
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(List(SuggestItem("as", SpecialTableConst.KEY_WORD_TABLE, Map())), tokens, tokenPos)
  }
}

/**
 * Suggests saving modes, including "overwrite", "append", "errorIfExists", and "ignore".
 */
private class SaveModeSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "mode"

  override def isMatch(): Boolean = {
    backOneStepIs(DSLSQLLexer.SAVE)
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
 * Suggests saving formats.
 */
private class SaveFormatSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "format"

  override def isMatch(): Boolean = {
    backOneStepIs(DSLSQLLexer.AS)
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
 * Suggests saving options, i.e. where/options statement contents.
 */
private class SaveOptionsSuggester(saveSuggester: SaveSuggester) extends SaveSuggesterBase(saveSuggester) {
  override def name: String = "options"

  // save overwrite command as parquet.`/tmp`
  override def isMatch(): Boolean = {
    if (backOneStepIs(DSLSQLLexer.BACKQUOTED_IDENTIFIER)) {
      return true
    }
    backAndFirstIs(DSLSQLLexer.OPTIONS) || backAndFirstIs(DSLSQLLexer.WHERE)
  }

  override def suggest(): List[SuggestItem] = {

    if (backOneStepIs(DSLSQLLexer.BACKQUOTED_IDENTIFIER)) {
      return LexerUtils.filterPrefixIfNeeded(getOptionsKeywords, tokens, tokenPos)
    }

    if (isOptionKey) {
      val asToken = firstAhead(DSLSQLLexer.AS)
      return asToken match {
        case Some(pos) =>
          // Fetch the saving format.
          val formatToken = tokens(pos + 1)

          // Fetch the names and descriptions of all data sources which will be suggested.
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

    if (isQuoteShouldPromb) {
      return getQuotes
    }

    if (isInQuote) {
      return List()
    }

    List()
  }
}


/**
 * Suggests "&#96;&#96;" after entering the saving format.
 * <br>Currently not effective since the front end does not send suggestion requests after typing a dot.
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

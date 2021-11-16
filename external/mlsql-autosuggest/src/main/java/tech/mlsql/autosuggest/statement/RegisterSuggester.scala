package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.commons.lang3.StringUtils
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}
import tech.mlsql.dsl.adaptor.MLMapping

import scala.collection.mutable

/**
 * 30/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class RegisterSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester
  with SuggesterRegister with StatementUtils {
  private val subInstances = new mutable.HashMap[String, StatementSuggester]()

  register(classOf[RegisterFormatSuggester])
  register(classOf[RegisterPathQuoteSuggester])
  register(classOf[RegisterPathSuggester])
  register(classOf[RegisterOptionsSuggester])

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[RegisterSuggester]).newInstance(this)
    subInstances.put(instance.name, instance)
    this
  }

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.REGISTER) => true
      case _ => false
    }
  }


  override def suggest(): List[SuggestItem] = {
    keywordSuggest ++ defaultSuggest(subInstances.toMap)
  }

  private def keywordSuggest: List[SuggestItem] = {
    var items = List[SuggestItem]()
    // If the where keyword already exists, it will not prompt
    if (backAndFirstIs(DSLSQLLexer.WHERE) || backAndFirstIs(DSLSQLLexer.OPTIONS)) {
      return items
    }

    _tokenPos match {
      case TokenPos(pos, TokenPosType.NEXT, _) =>
        val temp = TokenMatcher(_tokens, pos).back.
          eat(Food(None, DSLSQLLexer.IDENTIFIER)).
          eat(Food(None, DSLSQLLexer.AS)).
          eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).
          build
        if (temp.isSuccess) {
          items = List(SuggestItem("where ", SpecialTableConst.KEY_WORD_TABLE, Map()),
            SuggestItem("options ", SpecialTableConst.KEY_WORD_TABLE, Map()))
        }
        items
      case TokenPos(pos, TokenPosType.CURRENT, _) =>
        val temp = TokenMatcher(_tokens, pos - 1).back.
          eat(Food(None, DSLSQLLexer.IDENTIFIER)).
          eat(Food(None, DSLSQLLexer.AS)).
          eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).
          build
        if (temp.isSuccess) {
          // The current pos is IDENTIFIER, and is the where prefix
          if ("where".startsWith(_tokens(pos).getText)) {
            items = List(SuggestItem("where ", SpecialTableConst.KEY_WORD_TABLE, Map()))
          } else if ("options".startsWith(_tokens(pos).getText)) {
            items = List(SuggestItem("options ", SpecialTableConst.KEY_WORD_TABLE, Map()))
          }
        }
        items
      case _ => List()
    }

  }

  def getAllETNames: Set[String] = {
    MLMapping.getAllETNames
  }

  def getETInstanceMapping: Map[String, SQLAlg] = {
    MLMapping.getETInstanceMapping
  }

  override def name: String = "register"

  override def tokens: List[Token] = _tokens

  override def tokenPos: TokenPos = _tokenPos
}


class RegisterFormatSuggester(registerSuggester: RegisterSuggester) extends SuggesterBase(registerSuggester._tokens, registerSuggester._tokenPos) {
  override def isMatch(): Boolean = {
    val etNames = registerSuggester.getAllETNames
    if (tokenPos.currentOrNext == TokenPosType.CURRENT && !etNames.exists(name =>
      name.startsWith(tokens(tokenPos.pos).getText))) {
      return false
    }

    var skipSize = 0
    if (tokenPos.currentOrNext == TokenPosType.CURRENT) {
      skipSize = 1
    }
    if (tokenPos.pos - skipSize < 0) {
      return false
    }
    TokenMatcher(tokens, tokenPos.pos - skipSize).back.eat(Food(None, DSLSQLLexer.REGISTER)).build.isSuccess
  }

  override def suggest(): List[SuggestItem] = {
    // ET type suggest
    val etNames = registerSuggester.getAllETNames
    LexerUtils.filterPrefixIfNeeded(
      etNames.map(name => SuggestItem(s"$name.", SpecialTableConst.ET_TABLE,
        Map("desc" -> "ET"))).toList,
      tokens, tokenPos)

  }

  override def name: String = "format"
}


class RegisterPathQuoteSuggester(registerSuggester: RegisterSuggester) extends SuggesterBase(registerSuggester._tokens, registerSuggester._tokenPos) {


  override def isMatch(): Boolean = {
    val isMatched = isMatchWithoutBackQuoted
    if (!isMatched && tokenPos.currentOrNext == TokenPosType.CURRENT) {
      return TokenMatcher(tokens, tokenPos.pos).back.
        eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).
        eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
        eat(Food(None, DSLSQLLexer.IDENTIFIER)).
        eat(Food(None, DSLSQLLexer.REGISTER)).
        build.isSuccess
    }
    isMatched
  }

  override def suggest(): List[SuggestItem] = {
    val withoutBackQuoted = isMatchWithoutBackQuoted
    val skipSize = if (withoutBackQuoted) 0 else 1

    val res = TokenMatcher(tokens, tokenPos.pos - skipSize).back.
      eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
      eat(Food(None, DSLSQLLexer.IDENTIFIER)).
      build.getMatchTokens.last.getText match {
      case "WaterMarkInPlace" => registerSuggester.context.metaProvider
        .list().map(x => SuggestItem(s"`${x.key.table}`"
        , SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table")))
      case "ScriptUDF" =>
        LexerUtils.filterPrefixIfNeeded(List(SuggestItem("``", SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table"))),
          tokens, tokenPos)
      case _ =>
        registerSuggester.context.modelProvider
          .list().map(x => SuggestItem(s"`${x.key.pathQuote}`"
          , SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table")))
    }
    if (withoutBackQuoted)
      res.map(x => SuggestItem(s"${x.name} as ",
        SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table")))
    else LexerUtils.cleanTokenPrefix(tokens(tokenPos.pos).getText, res)
  }

  def isMatchWithoutBackQuoted: Boolean = {
    TokenMatcher(tokens, tokenPos.pos).back.
      eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
      eat(Food(None, DSLSQLLexer.IDENTIFIER)).
      eat(Food(None, DSLSQLLexer.REGISTER)).
      build.isSuccess
  }

  override def name: String = "pathQuote"
}

//Here you can implement Hive table / HDFS Path auto suggestion
class RegisterPathSuggester(registerSuggester: RegisterSuggester) extends SuggesterBase(registerSuggester._tokens, registerSuggester._tokenPos) {

  override def isMatch(): Boolean = {
    false
  }

  override def suggest(): List[SuggestItem] = {
    List()
  }

  override def name: String = "path"

}

class RegisterOptionsSuggester(registerSuggester: RegisterSuggester) extends SuggesterBase(registerSuggester._tokens, registerSuggester._tokenPos) {

  val systemRegisters = List("ScriptUDF", "WaterMarkInPlace")

  override def isMatch(): Boolean = {
    backAndFirstIs(DSLSQLLexer.OPTIONS) || backAndFirstIs(DSLSQLLexer.WHERE)
  }

  override def suggest(): List[SuggestItem] = {
    var mapping: Map[String, SQLAlg] = null
    if (tokens.size > 2) {
      mapping = registerSuggester.getETInstanceMapping
      if (StringUtils.isEmpty(tokens(1).getText) || !mapping.contains(tokens(1).getText)) {
        return List()
      }
    } else {
      return List()
    }

    var res: List[SuggestItem] = List()
    val keywords: List[SuggestItem] = List(SuggestItem("and ", SpecialTableConst.KEY_WORD_TABLE, Map("desc" -> "and")))
    val etName = tokens(1).getText
    if (!systemRegisters.contains(etName)) {
      return keywords ++ List(
        SuggestItem("`algIndex`=", SpecialTableConst.KEY_WORD_TABLE, Map("desc" -> "Manually select which set of parameters I use to get the algorithm")),
        SuggestItem("`autoSelectByMetric`=", SpecialTableConst.KEY_WORD_TABLE, Map("desc" -> "Let the system choose automatically, provided that we have configured evalateTable during training"))
      )
    }

    val etParams = mapping(etName).explainParams(registerSuggester.context.session).collect().
      map(row => (row.getString(0), row.getString(1))).
      toBuffer

    if (tokenPos.currentOrNext == TokenPosType.CURRENT) {
      res = LexerUtils.filterPrefixIfNeeded(etParams.map(tuple =>
        SuggestItem(s"${tuple._1}=", SpecialTableConst.OPTION_TABLE, Map("desc" -> tuple._2))).toList,
        tokens, tokenPos)
    } else if (tokens(tokenPos.pos).getType.equals(DSLSQLLexer.UNRECOGNIZED)) {
      if (tokens(tokenPos.pos).getText.equals("`")) {
        res = LexerUtils.filterPrefixIfNeeded(etParams.map(tuple =>
          SuggestItem(s"${tuple._1}`=", SpecialTableConst.OPTION_TABLE, Map("desc" -> tuple._2))).toList,
          tokens, tokenPos)
      }
    } else if (tokenPos.currentOrNext == TokenPosType.NEXT) {
      res = LexerUtils.filterPrefixIfNeeded(etParams.map(tuple =>
        SuggestItem(s"`${tuple._1}`=", SpecialTableConst.OPTION_TABLE, Map("desc" -> tuple._2))).toList,
        tokens, tokenPos)
    }
    res ++ keywords
  }

  override def name: String = "options"
}
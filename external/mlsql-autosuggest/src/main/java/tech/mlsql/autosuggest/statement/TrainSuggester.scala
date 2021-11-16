package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.statement.TrainSuggester.isTrainHeader
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}
import tech.mlsql.dsl.adaptor.MLMapping

import scala.collection.mutable

/**
 * 21/10/2021 hellozepp(lisheng.zhanglin@163.com)
 */
class TrainSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester
  with SuggesterRegister with StatementUtils {

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()

  register(classOf[TrainTempTableSuggester])
  register(classOf[TrainFormatSuggester])
  register(classOf[TrainPathQuoteSuggester])
  register(classOf[TrainPathSuggester])
  register(classOf[TrainOptionsSuggester])

  override def register(clazz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clazz.getConstructor(classOf[TrainSuggester]).newInstance(this)
    subInstances.put(instance.name, instance)
    this
  }

  override def isMatch(): Boolean = isTrainHeader(_tokens)


  override def suggest(): List[SuggestItem] = {
    keywordSuggest ++ defaultSuggest(subInstances.toMap)
  }

  private def keywordSuggest: List[SuggestItem] = {
    var items = List[SuggestItem]()
    // If the where keyword already exists, it will not prompt
    if (backAndFirstIs(DSLSQLLexer.WHERE) || backAndFirstIs(DSLSQLLexer.OPTIONS) || backAndFirstIs(DSLSQLLexer.PREDICT)) {
      return items
    }

    _tokenPos match {
      case TokenPos(pos, TokenPosType.NEXT, _) =>
        val temp = TokenMatcher(_tokens, pos).back.
          eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).
          eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
          build
        if (temp.isSuccess) {
          items = List(SuggestItem("where ", SpecialTableConst.KEY_WORD_TABLE, Map()),
            SuggestItem("options ", SpecialTableConst.KEY_WORD_TABLE, Map()))
        }
        items
      case TokenPos(pos, TokenPosType.CURRENT, _) =>
        val temp = TokenMatcher(_tokens, pos - 1).back.
          eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).
          eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
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

  override def name: String = "train"

  override def tokens: List[Token] = _tokens

  override def tokenPos: TokenPos = _tokenPos
}

object TrainSuggester {
  def isTrainHeader(_tokens: List[Token]): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.TRAIN) => true
      case Some(DSLSQLLexer.RUN) => true
      case Some(DSLSQLLexer.PREDICT) => true
      case _ => false
    }
  }
}

class TrainTempTableSuggester(trainSuggester: TrainSuggester) extends SuggesterBase(trainSuggester._tokens, trainSuggester._tokenPos) {
  override def isMatch(): Boolean = {
    if (tokenPos.currentOrNext == TokenPosType.CURRENT && trainSuggester.context.metaProvider.list().isEmpty) {
      // pass
    } else if (tokenPos.currentOrNext == TokenPosType.CURRENT && !trainSuggester.context.metaProvider.list().exists(table =>
      table.key.table.toUpperCase().startsWith(tokens(tokenPos.pos).getText.toUpperCase) ||
        "COMMAND".startsWith(tokens(tokenPos.pos).getText.toUpperCase))) {
      return false
    }

    var skipSize = 0
    if (tokenPos.currentOrNext == TokenPosType.CURRENT) {
      skipSize = 1
    }
    if (tokenPos.pos - skipSize < 0) {
      return false
    }
    TokenMatcher(tokens, tokenPos.pos - skipSize).back.eat(Food(None, DSLSQLLexer.TRAIN)).isSuccess ||
      TokenMatcher(tokens, tokenPos.pos - skipSize).back.eat(Food(None, DSLSQLLexer.RUN)).isSuccess ||
      TokenMatcher(tokens, tokenPos.pos - skipSize).back.eat(Food(None, DSLSQLLexer.PREDICT)).isSuccess
  }

  override def suggest(): List[SuggestItem] = {
    trainSuggester.context.metaProvider.list().filter(table => table.key.db == Option(SpecialTableConst.TEMP_TABLE_DB_KEY))
      .map(table => SuggestItem(table.key.table + " as ", table, Map("desc" -> "temp table"))) ++
      List(SuggestItem("command as ", SpecialTableConst.OTHER_TABLE, Map("desc" -> "command")))
  }

  override def name: String = "tempTable"
}

class TrainPathQuoteSuggester(trainSuggester: TrainSuggester) extends SuggesterBase(trainSuggester._tokens, trainSuggester._tokenPos) {

  def isMatchPathQouted(headToken: Int): Boolean = TokenMatcher(tokens, tokenPos.pos).back.
    eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
    eat(Food(None, DSLSQLLexer.IDENTIFIER)).
    eat(Food(None, DSLSQLLexer.AS)).
    eat(Food(None, DSLSQLLexer.IDENTIFIER)).
    eat(Food(None, headToken))
    .build.isSuccess

  override def isMatch(): Boolean = {
    isMatchPathQouted(DSLSQLLexer.RUN) || isMatchPathQouted(DSLSQLLexer.TRAIN) || isMatchPathQouted(DSLSQLLexer.PREDICT)
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(List(SuggestItem("``", SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table"))),
      tokens, tokenPos)
  }

  override def name: String = "pathQuote"
}

//Here you can implement Hive table / HDFS Path auto suggestion
class TrainPathSuggester(trainSuggester: TrainSuggester) extends SuggesterBase(trainSuggester._tokens, trainSuggester._tokenPos) {

  override def isMatch(): Boolean = {
    false
  }

  override def suggest(): List[SuggestItem] = {
    List()
  }

  override def name: String = "path"

}

class TrainFormatSuggester(trainSuggester: TrainSuggester) extends SuggesterBase(trainSuggester._tokens, trainSuggester._tokenPos) {
  override def isMatch(): Boolean = {
    val etNames = trainSuggester.getAllETNames
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
    TokenMatcher(tokens, tokenPos.pos - skipSize).back.eat(Food(None, DSLSQLLexer.AS), Food(None, DSLSQLLexer.IDENTIFIER))
      .eat(Food(None, DSLSQLLexer.RUN)).build.isSuccess ||
      TokenMatcher(tokens, tokenPos.pos - skipSize).back.eat(Food(None, DSLSQLLexer.AS), Food(None, DSLSQLLexer.IDENTIFIER))
        .eat(Food(None, DSLSQLLexer.TRAIN)).build.isSuccess||
      TokenMatcher(tokens, tokenPos.pos - skipSize).back.eat(Food(None, DSLSQLLexer.AS), Food(None, DSLSQLLexer.IDENTIFIER))
        .eat(Food(None, DSLSQLLexer.PREDICT)).build.isSuccess
  }

  override def suggest(): List[SuggestItem] = {
    // ET type suggest
    val etNames = trainSuggester.getAllETNames
    LexerUtils.filterPrefixIfNeeded(
      etNames.map(name => SuggestItem(s"$name.`` ", SpecialTableConst.ET_TABLE,
        Map("desc" -> "ET"))).toList,
      tokens, tokenPos)

  }

  override def name: String = "format"
}

class TrainOptionsSuggester(trainSuggester: TrainSuggester) extends SuggesterBase(trainSuggester._tokens, trainSuggester._tokenPos) {

  override def isMatch(): Boolean = {
    !backAndFirstIs(DSLSQLLexer.PREDICT) && (backAndFirstIs(DSLSQLLexer.OPTIONS) || backAndFirstIs(DSLSQLLexer.WHERE))
  }

  override def suggest(): List[SuggestItem] = {
    var mapping: Map[String, SQLAlg] = null
    if (tokens.size > 4) {
      mapping = trainSuggester.getETInstanceMapping
      if (!mapping.contains(tokens(3).getText)) {
        return List()
      }
    } else {
      return List()
    }
    val etName = tokens(3).getText
    val etParams = mapping(etName).explainParams(trainSuggester.context.session).collect().
      map(row => (row.getString(0), row.getString(1))).
      toBuffer

    var res: List[SuggestItem] = List()
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
    res ++ List(SuggestItem("and ", SpecialTableConst.KEY_WORD_TABLE, Map("desc" -> "and")),
      SuggestItem("as ", SpecialTableConst.KEY_WORD_TABLE, Map("desc" -> "as")))
  }

  override def name: String = "options"
}
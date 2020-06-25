package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.core.datasource.{DataSourceRegistry, MLSQLSourceInfo}
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, MLSQLTokenTypeWrapper, TokenMatcher}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}

import scala.collection.mutable

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 *
 *
 */
class LoadSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester
  with SuggesterRegister {

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()

  register(classOf[LoadPathSuggester])
  register(classOf[LoadFormatSuggester])
  register(classOf[LoadOptionsSuggester])
  register(classOf[LoadPathQuoteSuggester])

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[LoadSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.LOAD) => true
      case _ => false
    }
  }

  private def keywordSuggest: List[SuggestItem] = {
    _tokenPos match {
      case TokenPos(pos, TokenPosType.NEXT, offsetInToken) =>
        var items = List[SuggestItem]()
        val temp = TokenMatcher(_tokens, pos).back.
          eat(Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)).
          eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
          build
        if (temp.isSuccess) {
          items = List(SuggestItem("where", SpecialTableConst.KEY_WORD_TABLE, Map()), SuggestItem("as", SpecialTableConst.KEY_WORD_TABLE, Map()))
        }
        items

      case _ => List()
    }

  }

  override def suggest(): List[SuggestItem] = {
    keywordSuggest ++ defaultSuggest(subInstances.toMap)
  }


  override def name: String = "load"
}

class LoadFormatSuggester(loadSuggester: LoadSuggester) extends StatementSuggester with StatementUtils {
  override def isMatch(): Boolean = {

    (tokenPos.pos, tokenPos.currentOrNext) match {
      case (0, TokenPosType.NEXT) => true
      case (1, TokenPosType.CURRENT) => true
      case (_, _) => false
    }

  }

  override def suggest(): List[SuggestItem] = {
    // datasource type suggest
    val sources = (DataSourceRegistry.allSourceNames.toSet.toSeq ++ Seq(
      "parquet", "csv", "jsonStr", "csvStr", "json", "text", "orc", "kafka", "kafka8", "kafka9", "crawlersql", "image",
      "script", "hive", "xml", "mlsqlAPI", "mlsqlConf"
    )).toList
    LexerUtils.filterPrefixIfNeeded(
      sources.map(SuggestItem(_, SpecialTableConst.DATA_SOURCE_TABLE,
        Map("desc" -> "DataSource"))),
      tokens, tokenPos)

  }


  override def tokens: List[Token] = loadSuggester._tokens

  override def tokenPos: TokenPos = loadSuggester._tokenPos

  override def name: String = "format"
}

class LoadOptionsSuggester(loadSuggester: LoadSuggester) extends StatementSuggester with StatementUtils {
  override def isMatch(): Boolean = {
    backAndFirstIs(DSLSQLLexer.OPTIONS) || backAndFirstIs(DSLSQLLexer.WHERE)
  }

  override def suggest(): List[SuggestItem] = {
    val source = tokens(1)
    val datasources = DataSourceRegistry.fetch(source.getText, Map[String, String]()) match {
      case Some(ds) => ds.asInstanceOf[MLSQLSourceInfo].
        explainParams(loadSuggester.context.session).collect().
        map(row => (row.getString(0), row.getString(1))).
        toList
      case None => List()
    }
    LexerUtils.filterPrefixIfNeeded(datasources.map(tuple =>
      SuggestItem(tuple._1, SpecialTableConst.OPTION_TABLE, Map("desc" -> tuple._2))),
      tokens, tokenPos)

  }

  override def name: String = "options"

  override def tokens: List[Token] = loadSuggester._tokens

  override def tokenPos: TokenPos = loadSuggester._tokenPos
}

class LoadPathQuoteSuggester(loadSuggester: LoadSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "pathQuote"

  override def isMatch(): Boolean = {
    val temp = TokenMatcher(tokens, tokenPos.pos).back.
      eat(Food(None, MLSQLTokenTypeWrapper.DOT)).
      eat(Food(None, DSLSQLLexer.IDENTIFIER)).
      eat(Food(None, DSLSQLLexer.LOAD)).build
    temp.isSuccess
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(List(SuggestItem("``", SpecialTableConst.OTHER_TABLE, Map("desc" -> "path or table"))),
      tokens, tokenPos)
  }

  override def tokens: List[Token] = loadSuggester._tokens

  override def tokenPos: TokenPos = loadSuggester._tokenPos
}

//Here you can implement Hive table / HDFS Path auto suggestion
class LoadPathSuggester(loadSuggester: LoadSuggester) extends StatementSuggester with StatementUtils {
  override def isMatch(): Boolean = {
    false
  }

  override def suggest(): List[SuggestItem] = {
    List()
  }

  override def name: String = "path"


  override def tokens: List[Token] = loadSuggester._tokens

  override def tokenPos: TokenPos = loadSuggester._tokenPos
}



package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos}

import scala.collection.mutable

/**
 * connect jdbc where
 * as conn;
 */
class ConnectSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister with StatementUtils {
  private val subSuggesters = mutable.HashMap[String, StatementSuggester]()

  val formatMapping = Map(
    "jdbc" -> List(
      SuggestItem("url", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("driver", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("user", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("password", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("partitionColumn", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("lowerBound", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("upperBound", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("numPartitions", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("fetchsize", SpecialTableConst.OPTION_TABLE, Map("desc" -> "0")),
      SuggestItem("batchsize", SpecialTableConst.OPTION_TABLE, Map("desc" -> "1000")),
      SuggestItem("pushDownPredicate", SpecialTableConst.OPTION_TABLE, Map("desc" -> "true")),
      SuggestItem("pushDownAggregate", SpecialTableConst.OPTION_TABLE, Map("desc" -> "false")),
      SuggestItem("pushDownLimit", SpecialTableConst.OPTION_TABLE, Map("desc" -> "false")),
      SuggestItem("pushDownTableSample", SpecialTableConst.OPTION_TABLE, Map("desc" -> "false")),
      SuggestItem("pushDownTableSample", SpecialTableConst.OPTION_TABLE, Map("desc" -> "false")),
      SuggestItem("keytab", SpecialTableConst.OPTION_TABLE, Map("desc" -> "none")),
      SuggestItem("principal", SpecialTableConst.OPTION_TABLE, Map("desc" -> "none")),
      SuggestItem("refreshKrb5Config", SpecialTableConst.OPTION_TABLE, Map("desc" -> "none")),
      SuggestItem("connectionProvider", SpecialTableConst.OPTION_TABLE, Map("desc" -> "none")),
    )
  )


  override def name: String = "connect"

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.CONNECT) => true
      case _ => false
    }
  }

  override def suggest(): List[SuggestItem] = {
    defaultSuggest(subSuggesters.toMap)
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[ConnectSuggester]).newInstance(this)
    subSuggesters += instance.name -> instance
    this
  }

  register(classOf[ConnectFormatSuggester])
  register(classOf[ConnectOptionsSuggester])

  override def tokens: List[Token] = _tokens

  override def tokenPos: TokenPos = _tokenPos
}

private class ConnectFormatSuggester(connectSuggester: ConnectSuggester) extends StatementSuggester with StatementUtils {
  override def name: String = "format"

  /**
   *
   * connect j[cursor]
   * connect [cursor]
   */
  override def isMatch(): Boolean = {
    backOneStepIs(DSLSQLLexer.CONNECT)
  }

  override def suggest(): List[SuggestItem] = {
    val items = connectSuggester.formatMapping.map { item =>
      SuggestItem(item._1, SpecialTableConst.OPTION_TABLE, Map())
    }.toList
    LexerUtils.filterPrefixIfNeeded(items, tokens, tokenPos)
  }

  override def tokens: List[Token] = connectSuggester.tokens

  override def tokenPos: TokenPos = connectSuggester.tokenPos
}


private class ConnectOptionsSuggester(connectSuggester: ConnectSuggester) extends StatementSuggester with SuggesterRegister with StatementUtils {
  override def name: String = "options"

  private val subSuggesters = mutable.HashMap[String, StatementSuggester]()

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[ConnectSuggester]).newInstance(connectSuggester)
    subSuggesters += instance.name -> instance
    this
  }


  register(classOf[ConnectOptionKeySuggester])
  register(classOf[ConnectDriverValueSuggester])
  register(classOf[ConnectUrlValueSuggester])

  override def isMatch(): Boolean = {
    // connect jdbc where
    (backOneStepIs(DSLSQLLexer.IDENTIFIER) && backTwoStepIs(DSLSQLLexer.CONNECT)) ||
      (backAndFirstIs(DSLSQLLexer.OPTIONS) || backAndFirstIs(DSLSQLLexer.WHERE))
  }

  override def suggest(): List[SuggestItem] = {
    if (backOneStepIs(DSLSQLLexer.IDENTIFIER) && backTwoStepIs(DSLSQLLexer.CONNECT)) {
      return LexerUtils.filterPrefixIfNeeded(getOptionsKeywords, tokens, tokenPos)
    }
    defaultSuggest(subSuggesters.toMap)
  }

  override def tokens: List[Token] = connectSuggester.tokens

  override def tokenPos: TokenPos = connectSuggester.tokenPos
}

private class ConnectOptionKeySuggester(connectSuggester: ConnectSuggester) extends StatementSuggester with StatementUtils {

  override def name: String = "options_key"

  /**
   *
   * connect jdbc where
   * drive[cursor]
   *
   * connect jdbc where
   * `drive[cursor]
   *
   * * connect jdbc where url="" and
   * `drive[cursor]
   */
  override def isMatch(): Boolean = {
    isOptionKey
  }

  override def suggest(): List[SuggestItem] = {
    val format = tokens(1).getText
    val items = connectSuggester.formatMapping.getOrElse(format, List())
    LexerUtils.filterPrefixIfNeeded(items, tokens, tokenPos)
  }

  override def tokens: List[Token] = connectSuggester.tokens

  override def tokenPos: TokenPos = connectSuggester.tokenPos
}

private class ConnectUrlValueSuggester(connectSuggester: ConnectSuggester) extends StatementSuggester with StatementUtils {

  override def name: String = "url"

  /**
   *
   * connect jdbc where
   * driver="ab[cursor]"
   *
   * connect jdbc where
   * `driver`=""
   */
  override def isMatch(): Boolean = {
    isOptionValue && isOptionKeyEqual(name)
  }

  override def suggest(): List[SuggestItem] = {

    if (isQuoteShouldPromb) {
      LexerUtils.filterPrefixIfNeeded(getQuotes, tokens, tokenPos)
    }

    val items = List(
      SuggestItem("jdbc:mysql://<host>:<port>/<dbName>?useSSL=false&haracterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false", SpecialTableConst.OPTION_TABLE, Map("desc" -> "MySQL")),
      SuggestItem("jdbc:oracle:thin:@<host>:<port>:<dbName>", SpecialTableConst.OPTION_TABLE, Map("desc" -> "Oracle")),
      SuggestItem("dbc:hive2://<host>:<port>/<dbName>;<sessionConfs>?<hiveConfs>#<hiveVars>", SpecialTableConst.OPTION_TABLE, Map("desc" -> "Hive"))
    )
    LexerUtils.filterPrefixIfNeededWithStart(items, tokens, tokenPos, 1)
  }

  override def tokens: List[Token] = connectSuggester.tokens

  override def tokenPos: TokenPos = connectSuggester.tokenPos
}

private class ConnectDriverValueSuggester(connectSuggester: ConnectSuggester) extends StatementSuggester with StatementUtils {

  override def name: String = "driver"

  /**
   *
   * connect jdbc where
   * driver="ab[cursor]"
   *
   * connect jdbc where
   * `driver`=""
   */
  override def isMatch(): Boolean = {
    isOptionValue && isOptionKeyEqual(name)
  }

  override def suggest(): List[SuggestItem] = {
    val items =
      if (isInQuote && isOptionKeyEqual(name)) {
        List(
          SuggestItem("com.mysql.jdbc.Driver", SpecialTableConst.OPTION_TABLE, Map("desc" -> "MySQL")),
          SuggestItem("oracle.jdbc.driver.OracleDriver", SpecialTableConst.OPTION_TABLE, Map("desc" -> "Oracle")),
          SuggestItem("org.apache.hive.jdbc.HiveDriver", SpecialTableConst.OPTION_TABLE, Map("desc" -> "Hive"))
        )
      } else List(SuggestItem("\"\"", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")))
    LexerUtils.filterPrefixIfNeededWithStart(items, tokens, tokenPos, 1)
  }

  override def tokens: List[Token] = connectSuggester.tokens

  override def tokenPos: TokenPos = connectSuggester.tokenPos
}

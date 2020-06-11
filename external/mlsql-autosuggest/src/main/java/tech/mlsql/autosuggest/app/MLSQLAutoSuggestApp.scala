package tech.mlsql.autosuggest.app

import com.intigua.antlr4.autosuggest.{DefaultToCharStream, LexerWrapper, RawSQLToCharStream, ReflectionLexerAndParserFactory}
import org.apache.spark.sql.catalyst.parser.{SqlBaseLexer, SqlBaseParser}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLParser}
import tech.mlsql.app.CustomController
import tech.mlsql.autosuggest.AutoSuggestContext
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.version.VersionCompatibility

/**
 * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLAutoSuggestApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AutoSuggestContext.init
    AppRuntimeStore.store.registerController("autoSuggest", classOf[AutoSuggestController].getName)
  }

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0", "1.6.0-SNAPSHOT", "1.6.0")
  }
}

object AutoSuggestController {
  val lexerAndParserfactory = new ReflectionLexerAndParserFactory(classOf[DSLSQLLexer], classOf[DSLSQLParser]);
  val mlsqlLexer = new LexerWrapper(lexerAndParserfactory, new DefaultToCharStream)

  val lexerAndParserfactory2 = new ReflectionLexerAndParserFactory(classOf[SqlBaseLexer], classOf[SqlBaseParser]);
  val sqlLexer = new LexerWrapper(lexerAndParserfactory2, new RawSQLToCharStream)

}

class AutoSuggestController extends CustomController {
  override def run(params: Map[String, String]): String = {
    val sql = params("sql")
    val lineNum = params("lineNum").toInt
    val columnNum = params("columnNum").toInt
    val isDebug = params.getOrElse("isDebug", "false").toBoolean

    val context = new AutoSuggestContext(ScriptSQLExec.context().execListener.sparkSession,
      AutoSuggestController.mlsqlLexer,
      AutoSuggestController.sqlLexer)
    context.setDebugMode(isDebug)
    JSONTool.toJsonStr(context.buildFromString(sql).suggest(lineNum, columnNum))
  }
}

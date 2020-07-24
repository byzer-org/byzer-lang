package tech.mlsql.autosuggest.app

import com.intigua.antlr4.autosuggest.{DefaultToCharStream, LexerWrapper, RawSQLToCharStream, ReflectionLexerAndParserFactory}
import org.apache.spark.sql.catalyst.parser.{SqlBaseLexer, SqlBaseParser}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLParser}
import tech.mlsql.app.CustomController
import tech.mlsql.autosuggest.meta.RestMetaProvider
import tech.mlsql.autosuggest.{AutoSuggestContext, MLSQLSQLFunction}
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
    AppRuntimeStore.store.registerController("registerTable", classOf[RegisterTableController].getName)
    AppRuntimeStore.store.registerController("sqlFunctions", classOf[SQLFunctionController].getName)
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

  def getSchemaRegistry = {

    new SchemaRegistry(getSession)
  }

  def getSession = {
    val session = if (ScriptSQLExec.context() != null) ScriptSQLExec.context().execListener.sparkSession else Standalone.sparkSession
    session
  }
}

class SQLFunctionController extends CustomController {
  override def run(params: Map[String, String]): String = {
    JSONTool.toJsonStr(MLSQLSQLFunction.funcMetaProvider.list(Map()))
  }
}

class RegisterTableController extends CustomController {
  override def run(params: Map[String, String]): String = {
    def hasParam(str: String) = params.contains(str)

    def paramOpt(name: String) = {
      if (hasParam(name)) Option(params(name)) else None
    }

    val prefix = paramOpt("prefix")
    val db = paramOpt("db")
    require(hasParam("table"), "table is required")
    require(hasParam("schema"), "schema is required")
    val table = params("table")

    val session = AutoSuggestController.getSchemaRegistry
    params.getOrElse("schemaType", "") match {
      case Constants.DB => session.createTableFromDBSQL(prefix, db, table, params("schema"))
      case Constants.HIVE => session.createTableFromHiveSQL(prefix, db, table, params("schema"))
      case Constants.JSON => session.createTableFromJson(prefix, db, table, params("schema"))
      case _ => session.createTableFromDBSQL(prefix, db, table, params("schema"))
    }
    JSONTool.toJsonStr(Map("success" -> true))
  }
}

class AutoSuggestController extends CustomController {
  override def run(params: Map[String, String]): String = {
    val sql = params("sql")
    val lineNum = params("lineNum").toInt
    val columnNum = params("columnNum").toInt
    val isDebug = params.getOrElse("isDebug", "false").toBoolean
    val size = params.getOrElse("size", "30").toInt
    val includeTableMeta = params.getOrElse("includeTableMeta", "false").toBoolean

    val enableMemoryProvider = params.getOrElse("enableMemoryProvider", "true").toBoolean
    val session = AutoSuggestController.getSession
    val context = new AutoSuggestContext(session,
      AutoSuggestController.mlsqlLexer,
      AutoSuggestController.sqlLexer)
    context.setDebugMode(isDebug)
    if (enableMemoryProvider) {
      context.setUserDefinedMetaProvider(AutoSuggestContext.memoryMetaProvider)
    }

    val searchUrl = params.get("searchUrl")
    val listUrl = params.get("listUrl")
    (searchUrl, listUrl) match {
      case (Some(searchUrl), Some(listUrl)) =>
        context.setUserDefinedMetaProvider(new RestMetaProvider(searchUrl, listUrl))
      case (None, None) =>
    }
    var resItems = context.buildFromString(sql).suggest(lineNum, columnNum).take(size)
    if (!includeTableMeta) {
      resItems = resItems.map { item =>
        item.copy(metaTable = null)
      }.take(size)
    }
    JSONTool.toJsonStr(resItems)
  }
}

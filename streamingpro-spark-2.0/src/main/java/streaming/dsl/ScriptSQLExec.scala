package streaming.dsl

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import org.antlr.v4.runtime.tree.{ErrorNode, ParseTreeWalker, TerminalNode}
import org.antlr.v4.runtime.{ANTLRInputStream, CommonTokenStream, ParserRuleContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLListener, DSLSQLParser}
import streaming.dsl.parser.DSLSQLParser._

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 25/8/2017.
  */
object ScriptSQLExec {

  //dbName -> (format->jdbc,url->....)
  val dbMapping = new ConcurrentHashMap[String, Map[String, String]]()

  def options(name: String, _options: Map[String, String]) = {
    dbMapping.put(name, _options)
  }

  def main(args: Array[String]): Unit = {
    val input2 = "" +
      "select a as b from skone.kbs as k; " +
      "load jdbc.`mysql1.tb_v_user` as mysql_tb_user;" +
      "save csv_input_result as json.`/tmp/todd/result_json` partitionBy uid;" +
      "select a as j from b as k;"

    val input =
      """connect jdbc where driver="com.mysql.jdbc.Driver"
        |    and url="jdbc:mysql://127.0.0.1/alarm_test?characterEncoding=utf8"
        |    and user="root"
        |    and password="csdn.net"
        |    as db1;
        |load jdbc.`db1.t_report` as tr;
        |select * from tr  as new_tr;
        |save new_tr as json.`/tmp/todd`
        | """.stripMargin
    parse(input, new ScriptSQLExecListener(null, null, null))
    println(dbMapping)


  }

  def parse(input: String, listener: DSLSQLListener) = {
    val loadLexer = new DSLSQLLexer(new ANTLRInputStream(input))
    val tokens = new CommonTokenStream(loadLexer)
    val parser = new DSLSQLParser(tokens)
    val stat = parser.statement()
    ParseTreeWalker.DEFAULT.walk(listener, stat)
  }
}

class ScriptSQLExecListener(_sparkSession: SparkSession, _defaultPathPrefix: String, _allPathPrefix: Map[String, String]) extends DSLSQLListener {

  private val _env = new scala.collection.mutable.HashMap[String, String]
  private val lastSelectTable = new AtomicReference[String]()

  def setLastSelectTable(table: String) = {
    lastSelectTable.set(table)
  }

  def getLastSelectTable() = {
    if (lastSelectTable.get() == null) None else Some(lastSelectTable.get())
  }

  def addEnv(k: String, v: String) = {
    _env(k) = v
    this
  }

  def env() = _env

  def sparkSession = _sparkSession

  def pathPrefix(owner: Option[String]): String = {

    if (_allPathPrefix != null && _allPathPrefix.nonEmpty && owner.isDefined) {
      val pathPrefix = _allPathPrefix.get(owner.get)
      if (pathPrefix.isDefined && pathPrefix.get.endsWith("/")) {
        return pathPrefix.get
      } else {
        return pathPrefix.get + "/"
      }
    } else if (_defaultPathPrefix != null && _defaultPathPrefix.nonEmpty) {
      if (_defaultPathPrefix.endsWith("/")) {
        return _defaultPathPrefix
      } else {
        return _defaultPathPrefix + "/"
      }
    } else {
      return ""
    }
  }

  override def exitSql(ctx: SqlContext): Unit = {

    ctx.getChild(0).getText.toLowerCase() match {
      case "load" =>
        new LoadAdaptor(this).parse(ctx)

      case "select" =>
        new SelectAdaptor(this).parse(ctx)

      case "save" =>
        new SaveAdaptor(this).parse(ctx)

      case "connect" =>
        new ConnectAdaptor(this).parse(ctx)
      case "create" =>
        new CreateAdaptor(this).parse(ctx)
      case "insert" =>
        new InsertAdaptor(this).parse(ctx)
      case "drop" =>
        new DropAdaptor(this).parse(ctx)
      case "refresh" =>
        new RefreshAdaptor(this).parse(ctx)
      case "set" =>
        new SetAdaptor(this).parse(ctx)
      case "train" =>
        new TrainAdaptor(this).parse(ctx)
      case "register" =>
        new RegisterAdaptor(this).parse(ctx)
    }

  }

  override def enterStatement(ctx: StatementContext): Unit = {}

  override def exitStatement(ctx: StatementContext): Unit = {}

  override def enterSql(ctx: SqlContext): Unit = {}

  override def enterFormat(ctx: FormatContext): Unit = {}

  override def exitFormat(ctx: FormatContext): Unit = {}

  override def enterPath(ctx: PathContext): Unit = {}

  override def exitPath(ctx: PathContext): Unit = {}

  override def enterTableName(ctx: TableNameContext): Unit = {}

  override def exitTableName(ctx: TableNameContext): Unit = {}

  override def enterCol(ctx: ColContext): Unit = {}

  override def exitCol(ctx: ColContext): Unit = {}

  override def enterQualifiedName(ctx: QualifiedNameContext): Unit = {}

  override def exitQualifiedName(ctx: QualifiedNameContext): Unit = {}

  override def enterIdentifier(ctx: IdentifierContext): Unit = {}

  override def exitIdentifier(ctx: IdentifierContext): Unit = {}

  override def enterStrictIdentifier(ctx: StrictIdentifierContext): Unit = {}

  override def exitStrictIdentifier(ctx: StrictIdentifierContext): Unit = {}

  override def enterQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {}

  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {}

  override def visitTerminal(node: TerminalNode): Unit = {}

  override def visitErrorNode(node: ErrorNode): Unit = {}

  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}

  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}

  override def enterEnder(ctx: EnderContext): Unit = {}

  override def exitEnder(ctx: EnderContext): Unit = {}

  override def enterExpression(ctx: ExpressionContext): Unit = {}

  override def exitExpression(ctx: ExpressionContext): Unit = {}

  override def enterBooleanExpression(ctx: BooleanExpressionContext): Unit = {}

  override def exitBooleanExpression(ctx: BooleanExpressionContext): Unit = {}

  override def enterDb(ctx: DbContext): Unit = {}

  override def exitDb(ctx: DbContext): Unit = {}

  override def enterOverwrite(ctx: OverwriteContext): Unit = {}

  override def exitOverwrite(ctx: OverwriteContext): Unit = {}

  override def enterAppend(ctx: AppendContext): Unit = {}

  override def exitAppend(ctx: AppendContext): Unit = {}

  override def enterErrorIfExists(ctx: ErrorIfExistsContext): Unit = {}

  override def exitErrorIfExists(ctx: ErrorIfExistsContext): Unit = {}

  override def enterIgnore(ctx: IgnoreContext): Unit = {}

  override def exitIgnore(ctx: IgnoreContext): Unit = {}

  override def enterFunctionName(ctx: FunctionNameContext): Unit = {}

  override def exitFunctionName(ctx: FunctionNameContext): Unit = {}

  override def enterSetValue(ctx: SetValueContext): Unit = {}

  override def exitSetValue(ctx: SetValueContext): Unit = {}

  override def enterSetKey(ctx: SetKeyContext): Unit = {}

  override def exitSetKey(ctx: SetKeyContext): Unit = {}
}

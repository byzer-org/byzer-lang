package streaming.dsl

import java.util.concurrent.ConcurrentHashMap

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
        |""".stripMargin
    parse(input, new ScriptSQLExecListener(null))
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

class ScriptSQLExecListener(_sparkSession: SparkSession) extends DSLSQLListener {
  def sparkSession = _sparkSession

  override def exitSql(ctx: SqlContext): Unit = {

    ctx.getChild(0).getText match {
      case "load" =>
        new LoadAdaptor(this).parse(ctx)

      case "select" =>
        new SelectAdaptor(this).parse(ctx)

      case "save" =>
        new SaveAdaptor(this).parse(ctx)

      case "connect" =>
        new ConnectAdaptor(this).parse(ctx)
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
}

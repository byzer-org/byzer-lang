package streaming.dsl

import org.antlr.v4.runtime.tree.{ErrorNode, ParseTreeWalker, TerminalNode}
import org.antlr.v4.runtime.{ANTLRInputStream, CommonTokenStream, ParserRuleContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLListener, DSLSQLParser}
import streaming.dsl.parser.DSLSQLParser._

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 25/8/2017.
  */
object DSLSQL {
  def main(args: Array[String]): Unit = {
    val input = "select a as b from skone.kbs as k; load jdbc.`mysql1.tb_v_user` as mysql_tb_user;\nsave csv_input_result as json.`/tmp/todd/result_json` partitionBy uid;\nselect a as j from b as k;"
    parse(input, new DSLSQL(null))

  }

  def parse(input: String, listener: DSLSQLListener) = {
    val loadLexer = new DSLSQLLexer(new ANTLRInputStream(input))
    val tokens = new CommonTokenStream(loadLexer)
    val parser = new DSLSQLParser(tokens)
    val stat = parser.statement()
    ParseTreeWalker.DEFAULT.walk(listener, stat)
  }
}

class DSLSQL(sparkSession: SparkSession) extends DSLSQLListener {

  override def enterStatement(ctx: StatementContext): Unit = {}

  override def exitStatement(ctx: StatementContext): Unit = {}

  override def enterSql(ctx: SqlContext): Unit = {}

  override def exitSql(ctx: SqlContext): Unit = {
    ctx.getChild(0).getText match {
      case "load" =>
        val reader = sparkSession.read
        var table: DataFrame = null
        (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
          ctx.getChild(tokenIndex) match {
            case s: FormatContext =>
              if (s.getText == "jdbc") {
                //reader.jdbc()
              }
              else {
                reader.format(s.getText)
              }

            case s: PathContext =>
              table = reader.load(s.getText)
            case s: TableNameContext =>
              table.createOrReplaceTempView(s.getText)
          }
        }
      case "select" =>
        val chunks = new ArrayBuffer[String]()
        (0 to ctx.getChildCount - 1).foreach { index =>
          ctx.getChild(index).getText match {
            case "." =>
              chunks(index - 1) = chunks(index - 1) + s".${ctx.getChild(index + 1)}"
            case _ =>
              if (index == 0 || ctx.getChild(index - 1).getText != ".") {
                chunks += ctx.getChild(index).getText
              }

          }
        }
        val tableName = chunks.last
        val sql = (0 to chunks.length - 3).map(index => chunks(index)).mkString(" ")
          sparkSession.sql(sql).createOrReplaceTempView(tableName)

      case "save" =>

    }
  }

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
}

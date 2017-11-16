package streaming.dsl

import org.apache.spark.sql._
import _root_.streaming.dsl.parser.DSLSQLParser._

/**
  * Created by allwefantasy on 27/8/2017.
  */
class LoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    val reader = scriptSQLExecListener.sparkSession.read
    var table: DataFrame = null
    var format = ""
    var option = Map[String, String]()
    var path = ""
    var tableName = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          if (s.getText == "jdbc") {
            format = "jdbc"
          }
          if (s.getText == "es") {
            format = "org.elasticsearch.spark.sql"
          }
          reader.format(s.getText)
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> cleanStr(s.STRING().getText))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> cleanStr(s.expression().STRING().getText))
        case s: PathContext =>
          path = s.getText

        case s: TableNameContext =>
          tableName = s.getText
        case _ =>
      }
    }
    reader.options(option)
    format match {
      case "jdbc" =>
        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach { f =>
          reader.option(f._1, f._2)
        }
        reader.option("dbtable", dbAndTable(1))
        table = reader.load()

      case "es" | "org.elasticsearch.spark.sql" =>

        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach {
          f =>
            reader.option(f._1, f._2)
        }
        table = reader.load(dbAndTable(1))
      case "hbase" =>
        table = reader.load()
      case _ =>
        table = reader.load(withPathPrefix(scriptSQLExecListener.pathPrefix, cleanStr(path)))
    }
    table.createOrReplaceTempView(tableName)
  }
}

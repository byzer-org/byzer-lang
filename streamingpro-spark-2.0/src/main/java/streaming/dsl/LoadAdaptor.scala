package streaming.dsl

import org.apache.spark.sql._
import _root_.streaming.dsl.parser.DSLSQLParser.{FormatContext, PathContext, SqlContext, TableNameContext}

/**
  * Created by allwefantasy on 27/8/2017.
  */
class LoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    val reader = scriptSQLExecListener.sparkSession.read
    var table: DataFrame = null
    var format = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          if (s.getText == "jdbc") {
            format = "jdbc"
          }
          reader.format(s.getText)

        case s: PathContext =>
          if (format != "jdbc") {
            table = reader.load(withPathPrefix(scriptSQLExecListener.pathPrefix, cleanStr(s.getText)))
          }
          if (format == "jdbc") {
            val dbAndTable = cleanStr(s.getText).split("\\.")
            ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach { f =>
              reader.option(f._1, f._2)
            }
            reader.option("dbtable", dbAndTable(1))
            table = reader.load()
          }

        case s: TableNameContext =>
          table.createOrReplaceTempView(s.getText)

        case _ =>
      }
    }
  }
}

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
  }
}

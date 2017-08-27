package streaming.dsl

import org.apache.spark.sql._
import _root_.streaming.dsl.parser.DSLSQLParser.{FormatContext, PathContext, SqlContext, TableNameContext}

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SaveAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var writer: DataFrameWriter[Row] = null

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          writer.format(s.getText)
          writer.mode(SaveMode.Overwrite)

        case s: PathContext =>
          writer.save(s.getText)

        case s: TableNameContext =>
          writer = scriptSQLExecListener.sparkSession.sql(s.getText).write
      }
    }
  }
}

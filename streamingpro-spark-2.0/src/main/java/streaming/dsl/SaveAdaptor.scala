package streaming.dsl

import org.apache.spark.sql._
import _root_.streaming.dsl.parser.DSLSQLParser._

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SaveAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var writer: DataFrameWriter[Row] = null
    var mode = SaveMode.ErrorIfExists
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          writer.format(s.getText)

        case s: PathContext =>
          writer.save(withPathPrefix(scriptSQLExecListener.pathPrefix , cleanStr(s.getText)))

        case s: TableNameContext =>
          writer = scriptSQLExecListener.sparkSession.table(s.getText).write
          writer.mode(mode)
        case s: OverwriteContext =>
          mode = SaveMode.Overwrite
        case s: AppendContext =>
          mode = SaveMode.Append
        case s: ErrorIfExistsContext =>
          mode = SaveMode.ErrorIfExists
        case s: IgnoreContext =>
          mode = SaveMode.Ignore
        case s: ColContext =>
          writer.partitionBy(cleanStr(s.getText))
        case _ =>
      }
    }
  }
}

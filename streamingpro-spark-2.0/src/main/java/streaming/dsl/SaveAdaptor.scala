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
    var final_path = ""
    var format = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
          writer.format(s.getText)

        case s: PathContext =>
          format match {
            case "hive" => final_path = cleanStr(s.getText)
            case _ =>
              final_path = withPathPrefix(scriptSQLExecListener.pathPrefix, cleanStr(s.getText))
          }

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
          writer.partitionBy(cleanStr(s.getText).split(","): _*)
        case _ =>
      }
    }

    format match {
      case "hive" => writer.saveAsTable(final_path)
      case _ =>
        writer.save(final_path)
    }
  }
}

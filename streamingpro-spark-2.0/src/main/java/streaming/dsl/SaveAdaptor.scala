package streaming.dsl

import org.apache.spark.sql._
import _root_.streaming.dsl.parser.DSLSQLParser._
import _root_.streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SaveAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var writer: DataFrameWriter[Row] = null
    var oldDF: DataFrame = null
    var mode = SaveMode.ErrorIfExists
    var final_path = ""
    var format = ""
    var option = Map[String, String]()
    var tableName = ""
    var partitionByCol = Array[String]()

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
          format match {
            case "hive" =>
            case _ =>
              format = s.getText
          }


        case s: PathContext =>
          format match {
            case "hive" | "kafka8" | "kafka9" | "hbase" | "redis" | "es" =>
              final_path = cleanStr(s.getText)
            case _ =>
              final_path = withPathPrefix(scriptSQLExecListener.pathPrefix, cleanStr(s.getText))
          }

          final_path = TemplateMerge.merge(final_path, scriptSQLExecListener.env().toMap)

        case s: TableNameContext =>
          tableName = s.getText
          oldDF = scriptSQLExecListener.sparkSession.table(s.getText)
        case s: OverwriteContext =>
          mode = SaveMode.Overwrite
        case s: AppendContext =>
          mode = SaveMode.Append
        case s: ErrorIfExistsContext =>
          mode = SaveMode.ErrorIfExists
        case s: IgnoreContext =>
          mode = SaveMode.Ignore
        case s: ColContext =>
          partitionByCol = cleanStr(s.getText).split(",")
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> cleanStr(s.STRING().getText))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> cleanStr(s.expression().STRING().getText))
        case _ =>
      }
    }

    val dbAndTable = final_path.split("\\.")
    var connect_provied = false
    writer = oldDF.write
    if (dbAndTable.length == 2 && ScriptSQLExec.dbMapping.containsKey(dbAndTable(0))) {
      ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach {
        f =>
          writer.option(f._1, f._2)
      }
      connect_provied = true
    }

    if (connect_provied) {
      final_path = dbAndTable(1)
    }

    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
    }

    writer = writer.format(format).mode(mode).partitionBy(partitionByCol: _*).options(option)
    format match {
      case "es" =>
        writer.save(final_path)
      case "hive" =>
        writer.format(option.getOrElse("file_format", "parquet"))
        writer.saveAsTable(final_path)
      case "kafka8" | "kafka9" =>
        writer.option("topics", final_path).format("com.hortonworks.spark.sql.kafka08").save()
      case "hbase" =>
        writer.option("outputTableName", final_path).format("org.apache.spark.sql.execution.datasources.hbase").save()
      case "redis" =>
        writer.option("outputTableName", final_path).format("org.apache.spark.sql.execution.datasources.redis").save()
      case "mysql" =>
        writer.partitionBy(null).format("jdbc").option("dbtable", final_path).save()
      case _ =>
        writer.save(final_path)
    }
  }
}

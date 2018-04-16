package streaming.dsl

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import _root_.streaming.dsl.parser.DSLSQLParser._
import _root_.streaming.dsl.template.TemplateMerge
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SaveAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {

    var oldDF: DataFrame = null
    var mode = SaveMode.ErrorIfExists
    var final_path = ""
    var format = ""
    var option = Map[String, String]()
    var tableName = ""
    var partitionByCol = Array[String]()

    val owner = option.get("owner")

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
              final_path = withPathPrefix(scriptSQLExecListener.pathPrefix(owner), cleanStr(s.getText))
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

    if (scriptSQLExecListener.env().contains("stream")) {
      new StreamSaveAdaptor(option, oldDF, final_path, tableName, format, mode, partitionByCol).parse
    } else {
      new BatchSaveAdaptor(option, oldDF, final_path, tableName, format, mode, partitionByCol).parse
    }

  }
}

class BatchSaveAdaptor(var option: Map[String, String],
                       var oldDF: DataFrame,
                       var final_path: String,
                       var tableName: String,
                       var format: String,
                       var mode: SaveMode,
                       var partitionByCol: Array[String]
                      ) {
  def parse = {
    var writer = oldDF.write

    val dbAndTable = final_path.split("\\.")
    var connect_provied = false
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
        writer.option("outputTableName", final_path).format(
          option.getOrElse("implClass", "org.apache.spark.sql.execution.datasources.hbase")).save()
      case "redis" =>
        writer.option("outputTableName", final_path).format(
          option.getOrElse("implClass", "org.apache.spark.sql.execution.datasources.redis")).save()
      case _ =>
        writer.format(option.getOrElse("implClass", format)).save(final_path)
    }
  }
}

class StreamSaveAdaptor(var option: Map[String, String],
                        var oldDF: DataFrame,
                        var final_path: String,
                        var tableName: String,
                        var format: String,
                        var mode: SaveMode,
                        var partitionByCol: Array[String]
                       ) {
  def parse = {
    var writer: DataStreamWriter[Row] = oldDF.writeStream
    val dbAndTable = final_path.split("\\.")
    var connect_provied = false
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
    require(option.contains("checkpointLocation"), "checkpointLocation is required")
    require(option.contains("duration"), "duration is required")
    require(option.contains("mode"), "mode is required")


    writer = writer.format(format).outputMode(option("mode")).
      partitionBy(partitionByCol: _*).
      options((option - "mode" - "duration"))

    val dbtable = if (option.contains("dbtable")) option("dbtable") else final_path

    if (dbtable != null && dbtable != "-") {
      writer.option("path", dbtable)
    }

    writer.trigger(Trigger.ProcessingTime(option("duration").toInt, TimeUnit.SECONDS)).start()
  }
}

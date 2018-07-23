package streaming.dsl

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import _root_.streaming.dsl.parser.DSLSQLParser._
import _root_.streaming.dsl.template.TemplateMerge
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SaveAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

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
            case "hive" | "kafka8" | "kafka9" | "hbase" | "redis" | "es" | "jdbc" =>
              final_path = cleanStr(s.getText)
            case "parquet" | "json" | "csv" | "orc" =>
              final_path = withPathPrefix(scriptSQLExecListener.pathPrefix(owner), cleanStr(s.getText))
            case _ =>
              final_path = cleanStr(s.getText)
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
          option += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case _ =>
      }
    }

    if (scriptSQLExecListener.env().contains("stream")) {
      new StreamSaveAdaptor(scriptSQLExecListener, option, oldDF, final_path, tableName, format, mode, partitionByCol).parse
    } else {
      new BatchSaveAdaptor(scriptSQLExecListener, option, oldDF, final_path, tableName, format, mode, partitionByCol).parse
    }
    scriptSQLExecListener.setLastSelectTable(null)

  }
}

class BatchSaveAdaptor(val scriptSQLExecListener: ScriptSQLExecListener,
                       var option: Map[String, String],
                       var oldDF: DataFrame,
                       var final_path: String,
                       var tableName: String,
                       var format: String,
                       var mode: SaveMode,
                       var partitionByCol: Array[String]
                      ) {
  def parse = {

    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
    }

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
      case "jdbc" =>
        if (option.contains("idCol")) {
          import org.apache.spark.sql.jdbc.DataFrameWriterExtensions._
          val extraOptionsField = writer.getClass.getDeclaredField("extraOptions")
          extraOptionsField.setAccessible(true)
          val extraOptions = extraOptionsField.get(writer).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
          val jdbcOptions = new JDBCOptions(extraOptions.toMap + ("dbtable" -> final_path))
          writer.upsert(option.get("idCol"), jdbcOptions, oldDF)
        } else {
          writer.option("dbtable", final_path).save()
        }

      case "carbondata" =>
        if (dbAndTable.size == 2) {
          writer.option("tableName", dbAndTable(1)).option("dbName", dbAndTable(0))
        }
        if (dbAndTable.size == 1 && dbAndTable(0) != "-") {
          writer.option("tableName", dbAndTable(0))
        }
        writer.format(option.getOrElse("implClass", "org.apache.spark.sql.CarbonSource")).save()
      case _ =>
        if (final_path == "-" || final_path.isEmpty) {
          writer.format(option.getOrElse("implClass", format)).save()
        } else {
          writer.format(option.getOrElse("implClass", format)).save(final_path)
        }

    }
  }
}

class StreamSaveAdaptor(val scriptSQLExecListener: ScriptSQLExecListener,
                        var option: Map[String, String],
                        var oldDF: DataFrame,
                        var final_path: String,
                        var tableName: String,
                        var format: String,
                        var mode: SaveMode,
                        var partitionByCol: Array[String]
                       ) {
  def parse = {
    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
    }

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


    require(option.contains("checkpointLocation"), "checkpointLocation is required")
    require(option.contains("duration"), "duration is required")
    require(option.contains("mode"), "mode is required")

    writer = writer.format(option.getOrElse("implClass", format)).outputMode(option("mode")).
      partitionBy(partitionByCol: _*).
      options((option - "mode" - "duration"))

    val dbtable = if (option.contains("dbtable")) option("dbtable") else final_path

    if (dbtable != null && dbtable != "-") {
      writer.option("path", dbtable)
    }
    scriptSQLExecListener.env().get("streamName") match {
      case Some(name) => writer.queryName(name)
      case None =>
    }
    writer.trigger(Trigger.ProcessingTime(option("duration").toInt, TimeUnit.SECONDS)).start()
  }
}

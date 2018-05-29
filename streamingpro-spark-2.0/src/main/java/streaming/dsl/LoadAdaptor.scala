package streaming.dsl

import org.apache.spark.sql._
import _root_.streaming.dsl.parser.DSLSQLParser._
import template.TemplateMerge

/**
  * Created by allwefantasy on 27/8/2017.
  */
class LoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {

    var table: DataFrame = null
    var format = ""
    var option = Map[String, String]()
    var path = ""
    var tableName = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case s: PathContext =>
          path = s.getText

        case s: TableNameContext =>
          tableName = s.getText
        case _ =>
      }
    }


    if (format.startsWith("kafka")) {
      scriptSQLExecListener.addEnv("stream", "true")
      new StreamLoadAdaptor(scriptSQLExecListener, option, path, tableName, format).parse
    } else {
      new BatchLoadAdaptor(scriptSQLExecListener, option, path, tableName, format).parse
    }

  }
}

class BatchLoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener,
                       option: Map[String, String],
                       var path: String,
                       tableName: String,
                       format: String
                      ) extends DslTool {
  def parse = {
    var table: DataFrame = null
    val reader = scriptSQLExecListener.sparkSession.read
    reader.options(option)
    path = TemplateMerge.merge(path, scriptSQLExecListener.env().toMap)
    format match {
      case "jdbc" =>
        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach { f =>
          reader.option(f._1, f._2)
        }
        reader.option("dbtable", dbAndTable(1))
        table = reader.format("jdbc").load()

      case "es" | "org.elasticsearch.spark.sql" =>

        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach {
          f =>
            reader.option(f._1, f._2)
        }
        table = reader.format("org.elasticsearch.spark.sql").load(dbAndTable(1))
      case "hbase" | "org.apache.spark.sql.execution.datasources.hbase" =>
        table = reader.format("org.apache.spark.sql.execution.datasources.hbase").load()
      case "crawlersql" =>
        table = reader.option("path", cleanStr(path)).format("org.apache.spark.sql.execution.datasources.crawlersql").load()
      case "image" =>
        table = reader.option("path", cleanStr(path)).format("streaming.dsl.mmlib.algs.processing.image").load()
      case _ =>
        val owner = option.get("owner")
        table = reader.format(format).load(withPathPrefix(scriptSQLExecListener.pathPrefix(owner), cleanStr(path)))
    }
    table.createOrReplaceTempView(tableName)
  }
}

class StreamLoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener,
                        option: Map[String, String],
                        var path: String,
                        tableName: String,
                        format: String
                       ) extends DslTool {

  def withWaterMark(table: DataFrame, option: Map[String, String]) = {
    if (option.contains("eventTimeCol")) {
      table.withWatermark(option("eventTimeCol"), option("delayThreshold"))
    } else {
      table
    }

  }

  def parse = {
    var table: DataFrame = null
    val reader = scriptSQLExecListener.sparkSession.readStream

    format match {
      case "kafka" | "socket" =>
        table = reader.options(option).format(format).load()
      case "kafka8" | "kafka9" =>
        val format = "com.hortonworks.spark.sql.kafka08"
        /*
           kafka.bootstrap.servers
           kafka.metadata.broker
           startingoffset smallest
         */
        table = reader.format(format).options(option).load()
      case _ =>
    }
    table = withWaterMark(table, option)
    path = TemplateMerge.merge(path, scriptSQLExecListener.env().toMap)
    table.createOrReplaceTempView(tableName)
  }
}

package streaming.core.datasource

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrameWriter, Row}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.dsl.adaptor.DslTool

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
abstract class MLSQLBaseStreamSource extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with DslTool {

  def rewriteConfig(config: Map[String, String]) = {
    config
  }

  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    val oldDF = config.df.get
    var option = config.config
    if (option.contains("fileNum")) {
      option -= "fileNum"
    }

    val writer: DataStreamWriter[Row] = oldDF.writeStream

    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    var path = resolvePath(config.path, owner)

    var pathIsDBAndTable = false
    val Array(db, table) = parseRef(aliasFormat, path, dbSplitter, (options: Map[String, String]) => {
      writer.options(options)
      pathIsDBAndTable = true
    })

    if (pathIsDBAndTable) {
      path = table
    }


    require(option.contains("checkpointLocation"), "checkpointLocation is required")
    require(option.contains("duration"), "duration is required")
    require(option.contains("mode"), "mode is required")

    if (option.contains("partitionByCol")) {
      val cols = option("partitionByCol").split(",").filterNot(f => f.isEmpty)
      if (cols.size != 0) {
        writer.partitionBy(option("partitionByCol").split(","): _*)
      }
      option -= "partitionByCol"
    }

    val duration = option("duration").toInt
    option -= "duration"


    val mode = option("mode")
    option -= "mode"

    val format = config.config.getOrElse("implClass", fullFormat)

    //make sure the checkpointLocation is append PREFIX
    def rewriteOption = {
      val ckPath = option("checkpointLocation")
      option ++ Map("checkpointLocation" -> resourceRealPath(context.execListener, Option(context.owner), ckPath))
    }

    if (!skipFormat) {
      writer.format(format)
    }
    writer.outputMode(mode).options(rewriteOption)

    val dbtable = if (option.contains("dbtable")) option("dbtable") else path

    if (dbtable != null && dbtable != "-") {
      writer.option("path", dbtable)
    }

    context.execListener.env().get("streamName") match {
      case Some(name) => writer.queryName(name)
      case None =>
    }

    foreachBatchCallback(writer, option)

    writer.trigger(Trigger.ProcessingTime(duration, TimeUnit.SECONDS)).start()
  }

  def foreachBatchCallback(dataStreamWriter: DataStreamWriter[Row], options: Map[String, String]): Unit = {
    //do nothing by default
  }

  def skipFormat: Boolean = {
    false
  }


  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  def resolvePath(path: String, owner: String) = {
    path
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {

    val Array(db, table) = config.path.split("\\.") match {
      case Array(db, table) => Array(db, table)
      case Array(table) => Array("", table)
    }
    SourceInfo(shortFormat, db, table)
  }
}

package streaming.core.datasource

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrameWriter, Row}
import streaming.dsl.{ConnectMeta, DBMappingKey, DslTool, ScriptSQLExec}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
abstract class MLSQLBaseStreamSource extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with DslTool {

  def rewriteConfig(config: Map[String, String]) = {
    config
  }


  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    var oldDF = config.df.get
    var option = config.config
    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
      option -= "fileNum"
    }


    val writer: DataStreamWriter[Row] = oldDF.writeStream
    var path = config.path
    if (path.contains(".")) {
      val Array(_dbname, _dbtable) = path.split("\\.", 2)
      ConnectMeta.presentThenCall(DBMappingKey(shortFormat, _dbname), options => {
        path = _dbtable
        writer.options(options)
      })
    }

    require(option.contains("checkpointLocation"), "checkpointLocation is required")
    require(option.contains("duration"), "duration is required")
    require(option.contains("mode"), "mode is required")

    //    format match {
    //      case "jdbc" => writer.format("org.apache.spark.sql.execution.streaming.JDBCSinkProvider")
    //      /*
    //      Supports variable in path:
    //        save append post_parquet
    //        as parquet.`/post/details/hp_stat_date=${date.toString("yyyy-MM-dd")}`
    //       */
    //      case "newParquet" => writer.format("org.apache.spark.sql.execution.streaming.newfile")
    //      case _ => writer.format(option.getOrElse("implClass", format))
    //    }

    if (option.contains("partitionByCol")) {
      writer.partitionBy(option("partitionByCol").split(","): _*)
      option -= "partitionByCol"
    }

    val duration = option("duration").toInt
    option -= "duration"

    val checkpointLocation = option("checkpointLocation")
    option -= "checkpointLocation"

    val mode = option("mode")
    option -= "mode"

    val format = config.config.getOrElse("implClass", fullFormat)

    writer.format(format).outputMode(option("mode")).options(option)

    val dbtable = if (option.contains("dbtable")) option("dbtable") else path

    if (dbtable != null && dbtable != "-") {
      writer.option("path", dbtable)
    }

    ScriptSQLExec.contextGetOrForTest().execListener.env().get("streamName") match {
      case Some(name) => writer.queryName(name)
      case None =>
    }
    writer.trigger(Trigger.ProcessingTime(option("duration").toInt, TimeUnit.SECONDS)).start()
  }


  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {

    val Array(db, table) = config.path.split("\\.") match {
      case Array(db, table) => Array(db, table)
      case Array(table) => Array("", table)
    }
    SourceInfo(shortFormat, db, table)
  }
}

package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.datalake.DataLake

/**
 * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLHive(override val uid: String) extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with WowParams with Logging with WowLog {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {

    val dataLake = new DataLake(config.df.get.sparkSession)
    if (dataLake.isEnable && dataLake.overwriteHive && config.config.getOrElse("storage", "delta") == "delta") {
      val Array(db, table) = config.path.split("\\.")
      val deltaExists = dataLake.listTables.filter(f => f.database.get == db && f.table == table).size > 0
      if (deltaExists) {
        return reader.options(config.config).format(new MLSQLDelta().fullFormat).load(dataLake.identifyToPath(config.path))
      }
    }

    val format = config.config.getOrElse("implClass", fullFormat)
    reader.options(config.config).format(format).table(config.path)
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {

    val dataLake = new DataLake(config.df.get.sparkSession)
    if (dataLake.isEnable && dataLake.overwriteHive && config.config.getOrElse("storage", "delta") == "delta") {
      val partitionByCol = config.config.getOrElse("partitionByCol", "").split(",").filterNot(_.isEmpty)
      if (partitionByCol.length > 0) {
        writer.partitionBy(partitionByCol: _*)
      }
      writer.options(config.config).mode(config.mode).format(new MLSQLDelta().fullFormat).save(dataLake.identifyToPath(config.path))
      return
    }

    writer.format(config.config.getOrElse("file_format", "parquet"))
    val options = config.config - "file_format" - "implClass"
    config.config.get("partitionByCol").map(partitionColumn => partitionColumn.split(",").filterNot(_.isEmpty)).filterNot(_.length == 0)
      .map(partitionColumns => writer.partitionBy(partitionColumns: _*))
    writer.options(options).mode(config.mode).saveAsTable(config.path)
  }


  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(db, table) = config.path.split("\\.") match {
      case Array(db, table) => Array(db, table)
      case Array(table) => Array("default", table)
    }
    SourceInfo(shortFormat, db, table)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "hive"

  override def shortFormat: String = fullFormat
}

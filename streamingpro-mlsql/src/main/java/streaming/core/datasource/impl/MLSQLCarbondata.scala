package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLCarbondata(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val format = config.config.getOrElse("implClass", fullFormat)
    reader.options(config.config).format(format).table(config.path)
  }


  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val Array(db, table) = parseRef(shortFormat, config.path, dbSplitter, (options: Map[String, String]) => {
      writer.options(options)
    })

    if (db.isEmpty) {
      writer.option("tableName", table)
    } else {
      writer.option("tableName", db).option("dbName", table)
    }

    val format = config.config.getOrElse("implClass", fullFormat)
    writer.options(rewriteConfig(config.config)).format(format).save()
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(db, table) = parseRef(shortFormat, config.path, dbSplitter, (options: Map[String, String]) => {
    })
    SourceInfo(shortFormat, db, table)
  }

  override def fullFormat: String = "org.apache.spark.sql.CarbonSource"

  override def shortFormat: String = "carbondata"
}

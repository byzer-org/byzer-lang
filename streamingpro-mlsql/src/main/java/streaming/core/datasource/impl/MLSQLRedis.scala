package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLRedis(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "org.apache.spark.sql.execution.datasources.redis"

  override def shortFormat: String = "redis"

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    throw new RuntimeException(s"load is not supported in ${shortFormat}")
  }


  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {

    val Array(db, table) = parseRef(shortFormat, config.path, dbSplitter, (options: Map[String, String]) => {
      writer.options(options)
    })
    val format = config.config.getOrElse("implClass", fullFormat)
    writer.option("outputTableName", table)
    writer.options(rewriteConfig(config.config)).format(format).save()
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(db, table) = parseRef(shortFormat, config.path, dbSplitter, (options: Map[String, String]) => {
    })
    SourceInfo(shortFormat, db, table)
  }
}

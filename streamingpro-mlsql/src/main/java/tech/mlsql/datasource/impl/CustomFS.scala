package tech.mlsql.datasource.impl

import java.net.URI

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.dsl.adaptor.DslTool

/**
 * 1/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class CustomFS(override val uid: String) extends MLSQLSource
  with MLSQLSink
  with MLSQLSourceInfo
  with MLSQLSourceConfig
  with MLSQLRegistry with DslTool with WowParams {


  def this() = this(BaseParams.randomUID())
  
  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {

    require(URI.create(config.path).getScheme!=null,"path should be with schema specified")

    val session = config.df.get.sparkSession
    val (objectStoreConf,loadFileConf) = config.config.partition(item => item._1.startsWith("spark.hadoop") || item._1.startsWith("fs."))

    objectStoreConf.map { item =>
      if (item._1.startsWith("fs.")) {
        ("spark.hadoop." + item._1, item._2)
      } else item
    }.foreach(item => session.conf.set(item._1, item._2))

    val format = config.config.getOrElse("implClass", fullFormat)
    reader.options(loadFileConf).format(format).load(config.path)
    
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    require(URI.create(config.path).getScheme!=null,"path should be with schema specified")

    val session = config.df.get.sparkSession
    val (objectStoreConf,loadFileConf) = config.config.partition(item => item._1.startsWith("spark.hadoop") || item._1.startsWith("fs."))

    objectStoreConf.map { item =>
      if (item._1.startsWith("fs.")) {
        ("spark.hadoop." + item._1, item._2)
      } else item
    }.foreach(item => session.conf.set(item._1, item._2))
    

    val format = config.config.getOrElse("implClass", fullFormat)
    writer.options(loadFileConf).format(format).save(config.path)
  }
  

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def unRegister(): Unit = {
    DataSourceRegistry.unRegister(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType))
    DataSourceRegistry.unRegister(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType))
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    SourceInfo(shortFormat, "", config.path)
  }

  override def fullFormat: String = "FS"

  override def shortFormat: String = "FS"

  override def skipDynamicEvaluation = true

}

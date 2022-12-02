package tech.mlsql.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.{ConnectMeta, DBMappingKey}
import tech.mlsql.dsl.adaptor.DslTool

import scala.collection.mutable

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
    val session = config.df.get.sparkSession
    val extraObjectStoreConf = mutable.HashMap[String, String]()

    val realLoad = !config.path.isEmpty

    if (realLoad) {
      require(!config.path.startsWith("/") && !config.path.startsWith(".."), "path should be with schema specified")
      val schema = extractSchema(config.path)
      require(schema.isDefined, "path should be with schema specified")
      ConnectMeta.presentThenCall(DBMappingKey("FS", schema.get), kvs => {
        kvs.filter(_._1 != "format").foreach(kv => extraObjectStoreConf.put(kv._1, kv._2))
      })
    }

    // Object store config starts with spark.hadoop or fs
    val (objectStoreConf, loadFileConf) = (extraObjectStoreConf ++ config.config).partition(item => item._1.startsWith("spark.hadoop") || item._1.startsWith("fs."))

    objectStoreConf.map { item =>
      if (item._1.startsWith("spark.hadoop")) {
        (item._1.replaceAll("spark.hadoop.", ""), item._2)
      } else item
    }.foreach { item =>
      session.sparkContext.hadoopConfiguration.set(item._1, item._2)
    }
    // To load azure blob data, spark.hadoop prefix should be removed. Because NativeAzureFileSystem - azure blob's HDFS
    // compatible API, looks up Azure blob credentials by fs.azure.account.key.<account>.blob.core.chinacloudapi.cn.
    //    objectStoreConf.filter(_._1.startsWith("spark.hadoop.fs.azure")).foreach { conf =>
    //      session.sparkContext.hadoopConfiguration.set(conf._1.replace("spark.hadoop.fs.azure", "fs.azure"), conf._2)
    //    }

    if (realLoad) {
      val format = config.config.getOrElse("implClass", fullFormat)
      reader.options(loadFileConf - "implClass").format(format).load(config.path)
    } else {
      session.emptyDataFrame
    }

  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    val schema = extractSchema(config.path)
    require(schema.isDefined, "path should be with schema specified")

    val session = config.df.get.sparkSession

    val extraObjectStoreConf = mutable.HashMap[String, String]()

    ConnectMeta.presentThenCall(DBMappingKey("FS", schema.get), kvs => {
      kvs.filter(_._1 != "format").foreach(kv => extraObjectStoreConf.put(kv._1, kv._2))
    })


    val (objectStoreConf, loadFileConf) = (extraObjectStoreConf ++ config.config).partition(item => item._1.startsWith("spark.hadoop") || item._1.startsWith("fs."))


    objectStoreConf.map { item =>
      if (item._1.startsWith("spark.hadoop")) {
        (item._1.replaceAll("spark.hadoop.", ""), item._2)
      } else item
    }.foreach { item =>
      session.sparkContext.hadoopConfiguration.set(item._1, item._2)
    }
    // See comments in load method
    //    objectStoreConf.filter(_._1.startsWith("spark.hadoop.fs.azure")).foreach { conf =>
    //      session.conf.set(conf._1.replace("spark.hadoop.fs.azure", "fs.azure"), conf._2)
    //    }

    val format = config.config.getOrElse("implClass", fullFormat)
    writer.options(loadFileConf - "implClass").mode(config.mode).format(format).save(config.path)
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

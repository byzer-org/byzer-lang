package tech.mlsql.plugin.load

import org.apache.spark.sql.SparkSession
import streaming.core.datasource._
import streaming.dsl.{ConnectMeta, DBMappingKey, MLSQLExecuteContext}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun

/**
 * 28/7/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLMultiBucketSource extends RewritableSourceConfig with Logging {

  private val supportedFormats = Set("csv", "csvStr", "json", "jsonStr", "excel", "xml", "parquet", "text")

  override def rewrite_conf(config: DataSourceConfig, format: String, context: MLSQLExecuteContext): DataSourceConfig = {
    if (context.execListener.env().getOrElse("fileSystemMode", "default") != "multiBucket") {
      return config
    }
    if (!supportedFormats.contains(format)) {
      logInfo(s"$format is not in supported list(${supportedFormats.mkString(",")}) ")
      return config
    }
    val owner = context.owner

    var pathPrefix = ""
    // find the `connect` options and store them in session
    ConnectMeta.presentThenCall(DBMappingKey("FS", owner), options => {
      MLSQLMultiBucket.configFS(options, context.execListener.sparkSession)
      pathPrefix = options("pathPrefix")
    })
    config.copy(config.path, config.config + ("pathPrefix" -> pathPrefix))
  }

  override def rewrite_source(sourceInfo: SourceInfo, format: String, context: MLSQLExecuteContext): SourceInfo = {
    sourceInfo
  }
}

class MLSQLMultiBucketSink extends RewritableSinkConfig with Logging {

  private val supportedFormats = Set("csv", "json", "excel", "xml", "parquet")

  override def rewrite(config: DataSinkConfig, format: String, context: MLSQLExecuteContext): DataSinkConfig = {
    if (context.execListener.env().getOrElse("fileSystemMode", "default") != "multiBucket") {
      return config
    }
    if (!supportedFormats.contains(format)) {
      logInfo(s"$format is not in supported list(${supportedFormats.mkString(",")}) ")
      return config
    }
    val owner = context.owner

    var pathPrefix = ""
    // find the `connect` options and store them in session
    ConnectMeta.presentThenCall(DBMappingKey("FS", owner), options => {
      MLSQLMultiBucket.configFS(options, context.execListener.sparkSession)
      pathPrefix = options("pathPrefix")
    })
    config.copy(path = PathFun.joinPath(pathPrefix, config.path))
  }
}

class MLSQLMultiBucketFS extends RewritableFSConfig with Logging {

  override def rewrite(config: FSConfig, context: MLSQLExecuteContext): FSConfig = {
    if (context.execListener.env().getOrElse("fileSystemMode", "default") != "multiBucket") {
      return config
    }
    val owner = context.owner
    var pathPrefix = ""
    // find the `connect` options and store them in session
    ConnectMeta.presentThenCall(DBMappingKey("FS", owner), options => {
      MLSQLMultiBucket.configFS(options, context.execListener.sparkSession)
      pathPrefix = options("pathPrefix")
    })
    config.copy(path = PathFun.joinPath(pathPrefix, config.path))
  }
}

object MLSQLMultiBucket {
  def configFS(config: Map[String, String], session: SparkSession) = {
    val (objectStoreConf, _) = config.partition(item => item._1.startsWith("spark.hadoop") || item._1.startsWith("fs."))

    objectStoreConf.map { item =>
      if (item._1.startsWith("spark.hadoop")) {
        (item._1.replaceAll("spark.hadoop.", ""), item._2)
      } else item
    }.foreach { item =>
      session.sparkContext.hadoopConfiguration.set(item._1, item._2)
    }

    // To load azure blob data, spark.hadoop prefix should be removed. Because NativeAzureFileSystem - azure blob's HDFS
    // compatible API, looks up Azure blob credentials by fs.azure.account.key.<account>.blob.core.chinacloudapi.cn.
    objectStoreConf.filter(_._1.startsWith("spark.hadoop.fs.azure")).foreach { conf =>
      session.conf.set(conf._1.replace("spark.hadoop.fs.azure", "fs.azure"), conf._2)
    }
  }
}

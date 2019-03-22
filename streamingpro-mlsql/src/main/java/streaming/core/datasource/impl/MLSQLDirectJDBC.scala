package streaming.core.datasource.impl

import net.sf.json.JSONObject
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.{ConnectMeta, DBMappingKey}

import scala.collection.JavaConverters._

/**
  * 2018-12-21 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLDirectJDBC extends MLSQLDirectSource with MLSQLDirectSink with MLSQLSourceInfo with MLSQLRegistry {

  override def fullFormat: String = "jdbc"

  override def shortFormat: String = fullFormat

  override def dbSplitter: String = "."

  def toSplit = "\\."


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    var _params = config.config
    val path = config.path
    if (path.contains(".")) {
      val Array(db, table) = path.split("\\.", 2)
      ConnectMeta.presentThenCall(DBMappingKey("jdbc", db), options => {
        options.foreach { item =>
          _params += (item._1 -> item._2)
        }
      })
    }
    val res = JDBCUtils.executeQueryInDriver(_params ++ Map("driver-statement-query" -> config.config("directQuery")))
    val spark = config.df.get.sparkSession
    val rdd = spark.sparkContext.parallelize(res.map(item => JSONObject.fromObject(item.asJava).toString()))
    spark.read.json(rdd)
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    throw new MLSQLException("not support yet....")
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLDirectDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLDirectDataSourceType), this)
  }

  def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(toSplit, 2)
    } else {
      Array("", config.path)
    }

    val url = if (config.config.contains("url")) {
      config.config.get("url").get
    } else {
      val format = config.config.getOrElse("implClass", fullFormat)

      ConnectMeta.options(DBMappingKey(format, _dbname)) match {
        case Some(item) => item("url")
        case None => throw new RuntimeException(
          s"""
             |format: ${format}
             |ref:${_dbname}
             |However ref is not found,
             |Have you  set the connect statement properly?
           """.stripMargin)
      }
    }

    val dataSourceType = url.split(":")(1)
    val dbName = url.substring(url.lastIndexOf('/') + 1).takeWhile(_ != '?')
    val si = SourceInfo(dataSourceType, dbName, "")
    SourceTypeRegistry.register(dataSourceType, si)
    si
  }


}

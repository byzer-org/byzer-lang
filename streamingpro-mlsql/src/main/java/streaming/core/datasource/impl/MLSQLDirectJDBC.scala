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
class MLSQLDirectJDBC extends MLSQLDirectSource with MLSQLDirectSink with MLSQLRegistry {

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

}

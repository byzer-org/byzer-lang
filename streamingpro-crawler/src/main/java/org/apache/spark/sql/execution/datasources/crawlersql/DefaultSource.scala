package org.apache.spark.sql.execution.datasources.crawlersql

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import streaming.crawler.HttpClientCrawler
import us.codecraft.xsoup.Xsoup
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 2/4/2018.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    CrawlerSqlRelation(parameters, None)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    null
  }

  override def shortName(): String = "crawlersql"
}

case class CrawlerSqlRelation(
                               parameters: Map[String, String],
                               userSpecifiedschema: Option[StructType]
                             )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Logging {
  override def schema: StructType = {
    import org.apache.spark.sql.types._
    StructType(
      Array(
        StructField("root_url", StringType, false),
        StructField("url", StringType, false)
      )
    )

  }

  override def buildScan(): RDD[Row] = {
    val url = parameters("path")
    val matchXPath = parameters("matchXPath")
    val doc = HttpClientCrawler.request(url)
    val list = Xsoup.compile(matchXPath).evaluate(doc).list()
    val res = sqlContext.sparkContext.makeRDD(list).map(f => Row.fromSeq(Seq(url, f))).distinct()

//    //保存新抓取到的url
//    val tempStore = parameters.getOrElse("tempStore", "/tmp/streamingpro_crawler")
//    val mode = parameters.getOrElse("mode", "Append")
//    sqlContext.sparkSession.createDataFrame(res, schema).write.mode(SaveMode.valueOf(mode)).parquet(tempStore)
//    //返回结果
    res
  }
}

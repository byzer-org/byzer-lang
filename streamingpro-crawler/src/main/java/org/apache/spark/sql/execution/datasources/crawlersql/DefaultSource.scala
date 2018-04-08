package org.apache.spark.sql.execution.datasources.crawlersql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import streaming.crawler.{BrowserCrawler, HttpClientCrawler}
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

    //scroll/paging
    val pageType = parameters.getOrElse("page.type", "paging")
    val pageNum = parameters.getOrElse("page.num", "1").toInt
    val pageFlag = parameters.getOrElse("page.flag", "")

    val doc = if (pageType == "paging") {
      HttpClientCrawler.request(url)
    } else {
      BrowserCrawler.request(url, pageFlag, pageNum)
    }
    val list = Xsoup.compile(matchXPath).evaluate(doc).list()
    log.info(s"fetch $url result  size:" + list.size())
    //去重
    val res = sqlContext.sparkContext.makeRDD(list).map(f => Row.fromSeq(Seq(url, f))).distinct()

    //保存新抓取到的url
    val tempStore = parameters.getOrElse("tempStore", s"/tmp/streamingpro_crawler/${md5Hash(url)}")
    val fs = FileSystem.get(new Configuration())

    var result = res
    if (fs.exists(new Path(tempStore))) {
      val df_name = md5Hash(url) + System.currentTimeMillis()
      sqlContext.sparkSession.createDataFrame(res, schema).createOrReplaceTempView(df_name)
      sqlContext.sparkSession.read.parquet(tempStore).createOrReplaceTempView("url_history")

      //过滤历史的
      result = sqlContext.sparkSession.sql(
        s"""
           |select aut.url as url ,aut.root_url as root_url from ${df_name} aut
           |left join url_history auh
           |on aut.url=auh.url
           |where auh.url is null
      """.stripMargin).rdd
    }

    log.info(s"filtered fetch $url  result  size:" + result.count())

    //返回结果
    result
  }

  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }
}



/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.crawlersql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.jsoup.Jsoup
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
    val pageScrollTime = parameters.getOrElse("page.scroll.time", "1000").toInt

    // engine
    val engineType = parameters.getOrElse("engineType", "browser")

    // jsEnginePath
    val ptPath = parameters.getOrElse("jsEnginePath", "")

    // saveScreen
    val saveScreen = parameters.getOrElse("saveScreen", "false").toBoolean

    val webPage = if (engineType == "browser") {
      BrowserCrawler.request(url = url,
        ptPath = ptPath,
        c_flag = pageFlag,
        pageNum = pageNum,
        pageScrollTime = pageScrollTime,
        saveScreen = saveScreen)
    } else {
      HttpClientCrawler.request(url)
    }

    val doc = Jsoup.parse(webPage.pageSource)
    val list = Xsoup.compile(matchXPath).evaluate(doc).list()
    log.info(s"fetch $url result  size:" + list.size())

    val res = sqlContext.sparkContext.makeRDD(list).map(f => Row.fromSeq(Seq(url, f))).distinct()
    var result = res
    //保存新抓取到的url,去重
    if (parameters.containsKey("tempStore")) {
      val tempStore = parameters("tempStore")
      val fs = FileSystem.get(new Configuration())

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
    }

    //返回结果
    result
  }

  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }
}



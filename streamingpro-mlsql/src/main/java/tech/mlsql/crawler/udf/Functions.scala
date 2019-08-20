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

package tech.mlsql.crawler.udf

import cn.edu.hfut.dmic.contentextractor.ContentExtractor
import org.apache.spark.sql.UDFRegistration
import org.jsoup.Jsoup
import tech.mlsql.crawler.HttpClientCrawler
import us.codecraft.xsoup.Xsoup

/**
  * Created by allwefantasy on 3/4/2018.
  */
object Functions {
  def crawler_auto_extract_body(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_auto_extract_body", (co: String) => {
      if (co == null) null
      else ContentExtractor.getContentByHtml(co)
    })
  }

  def crawler_auto_extract_title(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_auto_extract_title", (co: String) => {
      if (co == null) null
      else {
        val doc = Jsoup.parse(co)
        doc.title()
      }

    })
  }

  def crawler_request(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_request", (co: String) => {
      val docStr = HttpClientCrawler.request(co)
      if (docStr != null) {
        val doc = Jsoup.parse(docStr.pageSource)
        if (doc == null) null
        else
          doc.html()
      } else null
    })
  }


  def crawler_request_image(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_request_image", (co: String) => {
      val image = HttpClientCrawler.requestImage(co)
      image
    })
  }


  def crawler_http(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_http", (url: String, method: String, items: Map[String, String]) => {
      HttpClientCrawler.requestByMethod(url, method, items)
    })
  }

  def crawler_extract_xpath(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_extract_xpath", (html: String, xpath: String) => {
      if (html == null) null
      else {
        val doc = Jsoup.parse(html)
        doc.title()
        Xsoup.compile(xpath).evaluate(doc).get()
      }

    })
  }

  def crawler_md5(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("crawler_md5", (html: String) => {
      if (html == null) null
      else {
        java.security.MessageDigest.getInstance("MD5").digest(html.getBytes()).map(0xFF & _).map {
          "%02x".format(_)
        }.foldLeft("") {
          _ + _
        }
      }

    })
  }

}

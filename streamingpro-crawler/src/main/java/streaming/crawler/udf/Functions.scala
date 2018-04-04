package streaming.crawler.udf

import cn.edu.hfut.dmic.contentextractor.ContentExtractor
import org.apache.spark.sql.UDFRegistration
import org.jsoup.Jsoup
import streaming.crawler.HttpClientCrawler
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
      val doc = HttpClientCrawler.request(co)
      if (doc == null) null
      else
        doc.html()
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
}

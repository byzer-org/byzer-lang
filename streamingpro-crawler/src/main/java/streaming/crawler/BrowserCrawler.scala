package streaming.crawler

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import org.openqa.selenium.{By, JavascriptExecutor, WebDriver}
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}

/**
  * Created by allwefantasy on 2/4/2018.
  */
object BrowserCrawler {

  val resource = new WebDriverResource()


  def request(url: String, ptPath: String, c_flag: String = "", pageNum: Int = 0, pageScrollTime: Int = 1000, timeout: Int = 10, useProxy: Boolean = false): Document = {
    var webDriver: WebDriver = null
    try {
      webDriver = resource.poll() //getPhantomJs(useProxy, ptPath)
      webDriver.get(url)
      val wait = new WebDriverWait(webDriver, timeout)
      if (!c_flag.isEmpty) {
        wait.until(ExpectedConditions.presenceOfElementLocated(By.id(c_flag)))
      }
      //---------------
      if (pageNum > 0) {
        val jse = webDriver.asInstanceOf[JavascriptExecutor]
        (0 until pageNum).foreach { f =>
          jse.executeScript(
            s"""
               |var scrollingElement = (document.scrollingElement || document.body);
               |window.scrollBy(0,scrollingElement.scrollHeight+300);
             """.stripMargin, "")
          Thread.sleep(pageScrollTime)
        }

      }
      //---------------

      val document = Jsoup.parse(webDriver.getPageSource())
      document
    } finally {
      if (webDriver != null) {
        resource.put(webDriver)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //println(request("https://wwww.baidu.com", "su").body())
  }
}

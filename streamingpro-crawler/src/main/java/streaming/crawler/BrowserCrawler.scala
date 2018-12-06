package streaming.crawler

import java.util.Base64

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.openqa.selenium.remote.RemoteWebDriver
import org.openqa.selenium.{By, JavascriptExecutor, OutputType, WebDriver}
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}
import streaming.crawler.beans.WebPage

/**
  * Created by allwefantasy on 2/4/2018.
  */
object BrowserCrawler {

  val resource = new WebDriverResource()


  def request(url: String,
              ptPath: String,
              c_flag: String = "",
              pageNum: Int = 0,
              pageScrollTime: Int = 1000,
              timeout: Int = 10,
              clientJs: String = null,
              saveScreen: Boolean = false,
              useProxy: Boolean = false): WebPage = {
    var webDriver: WebDriver = null
    try {
      webDriver = resource.poll() //getPhantomJs(useProxy, ptPath)
      webDriver.manage().window().maximize()
      webDriver.get(url)
      val wait = new WebDriverWait(webDriver, timeout)
      if (!c_flag.isEmpty) {
        wait.until(ExpectedConditions.presenceOfElementLocated(By.id(c_flag)))
      }
      val jse = webDriver.asInstanceOf[JavascriptExecutor]
      //---------------
      if (pageNum > 0) {
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
      if (clientJs != null) {
        jse.executeScript(clientJs)
      }
      var screen: String = null
      if (saveScreen && webDriver.isInstanceOf[RemoteWebDriver]) {
        val remoteWebDriver = webDriver.asInstanceOf[RemoteWebDriver]
        val bytes = remoteWebDriver.getScreenshotAs(OutputType.BYTES)
        screen = Base64.getEncoder.encodeToString(bytes)
      }
      WebPage(webDriver.getPageSource(), screen)
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

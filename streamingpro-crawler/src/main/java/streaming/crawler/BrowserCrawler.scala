package streaming.crawler

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.openqa.selenium.{By, Proxy, WebDriver}
import org.openqa.selenium.phantomjs.PhantomJSDriver
import org.openqa.selenium.remote.{CapabilityType, DesiredCapabilities}
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}

/**
  * Created by allwefantasy on 2/4/2018.
  */
object BrowserCrawler {
  def getPhantomJs(useProxy: Boolean): WebDriver = {
    ///usr/local/Cellar/phantomjs/2.1.1
    val ptPath = "phantomjs.binary.path"

    def isMac() = {
      val OS = System.getProperty("os.name").toLowerCase()
      OS.indexOf("mac") >= 0 && OS.indexOf("os") > 0 && OS.indexOf("x") > 0
    }
    if (isMac()) {
      System.setProperty("phantomjs.binary.path", "/usr/local/Cellar/phantomjs/2.1.1/bin/phantomjs")
    }
    if (!System.getProperties.containsKey(ptPath)) {
      throw new RuntimeException("phantomjs.binary.path is not set")
    }
    val desiredCapabilities = DesiredCapabilities.phantomjs()
    desiredCapabilities.setCapability("phantomjs.page.settings.userAgent", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:50.0) Gecko/20100101 Firefox/50.0")
    desiredCapabilities.setCapability("phantomjs.page.customHeaders.User-Agent", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:50.0) Gecko/20100101 　　Firefox/50.0")
    if (useProxy) {
      val proxy = new Proxy()
      proxy.setProxyType(org.openqa.selenium.Proxy.ProxyType.MANUAL)
      proxy.setAutodetect(false)
      var proxyStr = ""
      do {
        proxyStr = ProxyUtil.getProxy()
      } while (proxyStr.length == 0)

      proxy.setHttpProxy(proxyStr)
      desiredCapabilities.setCapability(CapabilityType.PROXY, proxy)
    }

    new PhantomJSDriver(desiredCapabilities)


  }

  def request(url: String, c_flag: String): Document = {
    var webDriver: WebDriver = null
    try {
      webDriver = getPhantomJs(false)
      webDriver.get(url)
      val wait = new WebDriverWait(webDriver, 10)
      wait.until(ExpectedConditions.presenceOfElementLocated(By.id(c_flag)))
      val document = Jsoup.parse(webDriver.getPageSource())
      document
    } finally {
      if (webDriver != null) {
        webDriver.quit()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(request("https://wwww.baidu.com", "su").body())
  }
}

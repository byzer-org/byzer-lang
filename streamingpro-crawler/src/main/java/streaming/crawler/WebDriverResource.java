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

package streaming.crawler;

import org.openqa.selenium.Capabilities;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.BrowserType;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.openqa.selenium.remote.CapabilityType.SUPPORTS_JAVASCRIPT;
import static org.openqa.selenium.remote.CapabilityType.TAKES_SCREENSHOT;

/**
 * Created by allwefantasy on 17/8/2018.
 */
public class WebDriverResource {

    public static final DesiredCapabilities DEFAULT_CAPABILITIES;
    public static final ChromeOptions DEFAULT_CHROME_CAPABILITIES;

    static {
        // HtmlUnit
        DEFAULT_CAPABILITIES = new DesiredCapabilities();
        DEFAULT_CAPABILITIES.setCapability(SUPPORTS_JAVASCRIPT, true);
        DEFAULT_CAPABILITIES.setCapability(TAKES_SCREENSHOT, false);
        DEFAULT_CAPABILITIES.setCapability("downloadImages", false);
        DEFAULT_CAPABILITIES.setCapability("browserLanguage", "zh_CN");
        DEFAULT_CAPABILITIES.setCapability("resolution", "1920x1080");

        // see https://peter.sh/experiments/chromium-command-line-switches/
        DEFAULT_CHROME_CAPABILITIES = new ChromeOptions();
        DEFAULT_CHROME_CAPABILITIES.merge(DEFAULT_CAPABILITIES);
        DEFAULT_CHROME_CAPABILITIES.setHeadless(true);
        DEFAULT_CHROME_CAPABILITIES.addArguments("--window-size=1920,1080");
    }

    private final Map<Integer, ArrayBlockingQueue<WebDriver>> freeDrivers = new HashMap<>();

    public WebDriver poll() {

        try {
            ArrayBlockingQueue<WebDriver> queue = freeDrivers.get(0);
            if (queue == null) {
                queue = new ArrayBlockingQueue<>(4);
                freeDrivers.put(0, queue);
            }

            if (queue.isEmpty()) {
                allocateWebDriver(queue);
            }

            WebDriver driver = queue.poll(2 * 2, TimeUnit.SECONDS);

            return driver;
        } catch (InterruptedException e) {
            System.out.println("Failed to poll a WebDriver from pool, " + e.toString());
        }

        // TODO: throw exception
        return null;
    }

    public void put(WebDriver driver) {
        try {
            freeDrivers.get(0).put(driver);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void allocateWebDriver(ArrayBlockingQueue<WebDriver> queue) {
        if (freeDrivers.get(0).size() > 10) {
            return;
        }

        try {
            WebDriver driver = doCreateWebDriver(true, BrowserType.CHROME, null, 3l, 3l, 3l);
            if (driver != null) {
                queue.put(driver);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            // throw new RuntimeException("Can not create WebDriver");
        }
    }

    private void close() {
        for (WebDriver webDriver : freeDrivers.get(0)) {
            try {
                webDriver.quit();
            } catch (Exception e) {

            }
        }
    }

    private WebDriver doCreateWebDriver(boolean headless, String browserType,
                                        Class<? extends WebDriver> defaultWebDriverClass,
                                        Long pageLoadTimeout,
                                        Long scriptTimeout,
                                        Long implicitlyWait

    )
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        DesiredCapabilities capabilities = new DesiredCapabilities(DEFAULT_CAPABILITIES);
        ChromeOptions chromeOptions = new ChromeOptions();
        chromeOptions.merge(DEFAULT_CHROME_CAPABILITIES);
        // Use headless mode by default, GUI mode can be used for debugging
        chromeOptions.setHeadless(headless);
        if (headless) {
            // Do not downloading images in headless mode
            chromeOptions.addArguments("--blink-settings=imagesEnabled=false");
        }

        // Reset proxy
        capabilities.setCapability(CapabilityType.PROXY, (Object) null);
        chromeOptions.setCapability(CapabilityType.PROXY, (Object) null);


        // Choose the WebDriver
        WebDriver driver;
        if (browserType == BrowserType.CHROME) {
            driver = new ChromeDriver(chromeOptions);
        } else {
            if (RemoteWebDriver.class.isAssignableFrom(defaultWebDriverClass)) {
                driver = defaultWebDriverClass.getConstructor(Capabilities.class).newInstance(capabilities);
            } else {
                driver = defaultWebDriverClass.getConstructor().newInstance();
            }
        }

        // Set timeouts
        WebDriver.Timeouts timeouts = driver.manage().timeouts();
        timeouts.pageLoadTimeout(pageLoadTimeout, TimeUnit.SECONDS);
        timeouts.setScriptTimeout(scriptTimeout, TimeUnit.SECONDS);
        timeouts.implicitlyWait(implicitlyWait, TimeUnit.SECONDS);

        // Set log level
        if (driver instanceof RemoteWebDriver) {
            RemoteWebDriver remoteWebDriver = (RemoteWebDriver) driver;

            final Logger webDriverLog = LoggerFactory.getLogger(WebDriver.class);
            Level level = Level.FINE;
            if (webDriverLog.isDebugEnabled()) {
                level = Level.FINER;
            } else if (webDriverLog.isTraceEnabled()) {
                level = Level.ALL;
            }

            remoteWebDriver.setLogLevel(level);
        }

        return driver;
    }

}

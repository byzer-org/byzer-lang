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

package streaming.core.strategy.platform

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.base.Strings
import com.google.common.cache._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import streaming.log.Logging


/**
  * Auther: daic
  * Description:    
  * Date：Create in 10:56 2019/4/15
  * Modified By:
  */
object SQLTableCacheClean extends Logging {

  var writeCleanUpDelay: Long = _
  var cleanUpCycle: Long = _

  private val cacheManager =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("SQLTableCacheClean").build())

  def start(): Unit = {
    cacheManager.scheduleAtFixedRate(cacheCleaner, 1000, cleanUpCycle, TimeUnit.SECONDS)

    def cacheCleaner = new Runnable {
      override def run(): Unit = {
        cache.cleanUp()
      }
    }
  }

  var cloneSparkSession: SparkSession = _

  val loader = new CacheLoader[String, String]() {
    override def load(key: String): (String) = {
      _load(key)
    }
  }

  def _load(cacheName: String): (String) = synchronized {
    ""
  }

  val cache: LoadingCache[String, String] = CacheBuilder.newBuilder()
    .concurrencyLevel(8)
    .initialCapacity(10)
    .maximumSize(1000)
    .recordStats()
    .expireAfterWrite(writeCleanUpDelay, TimeUnit.SECONDS)
    .removalListener(new CacheRemoveListener)
    .build(loader);

  class CacheRemoveListener extends RemovalListener[String, String] {

    override def onRemoval(notification: RemovalNotification[String, String]): Unit = {

      if (null != cloneSparkSession) {
        if (!Strings.isNullOrEmpty(notification.getValue) && RemovalCause.EXPIRED.equals(notification.getCause)) {
          cloneSparkSession.sqlContext.uncacheTable(notification.getValue)
          logInfo(notification.getKey() + " " + notification.getValue() + " was removed,the reason:" +
            notification.getCause())
        }
      }
    }
  }

}

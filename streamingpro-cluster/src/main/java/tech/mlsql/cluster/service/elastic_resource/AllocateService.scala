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

package tech.mlsql.cluster.service.elastic_resource

import java.util.concurrent.{Executors, TimeUnit}
import java.util.logging.Logger

import tech.mlsql.cluster.ProxyApplication
import tech.mlsql.cluster.model.ElasticMonitor
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
object AllocateService extends Logging {
  val logger = Logger.getLogger("AllocateService")
  private[this] val _executor = Executors.newFixedThreadPool(100)
  private[this] val scheduler = Executors.newSingleThreadScheduledExecutor()

  private[this] val mapping = Map(
    "JobNumAwareAllocate" -> new JobNumAwareAllocateStrategy()
  )

  def shutdown = {
    _executor.shutdownNow()
  }

  def run = {
    scheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        ElasticMonitor.items().asScala.foreach { monitor =>
          try {
            val allocateStrategy = mapping(monitor.getAllocateStrategy)
            val tags = monitor.getTag.split(",").toSeq
            val allocate = allocateStrategy.plan(tags, monitor)

            allocate match {
              case Some(item) =>
                _executor.execute(new Runnable {
                  override def run(): Unit = {
                    logDebug(s"check instances tagged with [$tags]. Allocate ${item}")
                    allocateStrategy.allocate(item, monitor)
                  }
                })
              case None =>
                logDebug(s"check instances tagged with [$tags]. Allocate None")
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
            //catch all ,so the scheduler will not been stopped by  exception
          }
        }
      }
    }, 60, ProxyApplication.commandConfig.allocateCheckInterval, TimeUnit.SECONDS)
  }
}



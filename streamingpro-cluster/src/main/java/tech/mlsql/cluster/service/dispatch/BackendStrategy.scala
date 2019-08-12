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

package tech.mlsql.cluster.service.dispatch

import tech.mlsql.cluster.service.BackendService.mapSResponseToObject
import tech.mlsql.cluster.service.{BackendCache, BackendService}
import tech.mlsql.common.utils.log.Logging

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
trait BackendStrategy {
  def invoke(backends: Seq[BackendCache]): Option[Seq[BackendCache]]

}

class AllBackendsStrategy(tags: String) extends BackendStrategy {
  override def invoke(backends: Seq[BackendCache]): Option[Seq[BackendCache]] = {
    val tagSet = tags.split(",").toSet
    if (tags.isEmpty) {
      Option(backends)
    } else {
      Option(backends.filter(f => tagSet.intersect(f.meta.getTag.split(",").toSet).size > 0))
    }

  }
}

class JobNumAwareStrategy(tags: String) extends BackendStrategy {
  override def invoke(backends: Seq[BackendCache]): Option[Seq[BackendCache]] = {
    val tagSet = tags.split(",").toSet
    var nonActiveBackend = BackendService.nonActiveBackend
    if (!tags.isEmpty) {
      nonActiveBackend = nonActiveBackend.filter(f => tagSet.intersect(f.getTag.split(",").toSet).size > 0)
    }
    val backend = if (nonActiveBackend.size > 0) {
      nonActiveBackend.headOption
    } else {
      var activeBackends = BackendService.activeBackend.toSeq
      if (!tags.isEmpty) {
        activeBackends = activeBackends.filter(f => tagSet.intersect(f._1.getTag.split(",").toSet).size > 0).sortBy(f => f._2)
      }
      activeBackends.headOption.map(f => f._1)

    }
    BackendService.find(backend).map(f => Seq(f))
  }
}

class ResourceAwareStrategy(tags: String) extends BackendStrategy with Logging {
  override def invoke(backends: Seq[BackendCache]): Option[Seq[BackendCache]] = {

    val tagSet = tags.split(",").toSet

    var backends = BackendService.backends
    if (!tags.isEmpty) {
      backends = backends.filter(f => tagSet.intersect(f.meta.getTag.split(",").toSet).size > 0)
    }
    val backend = backends.seq.map { b =>
      logDebug(s"visit backend: ${b.meta.getUrl} tags: ${b.meta.getTag}")
      var returnItem = (Long.MaxValue, b)
      try {
        val res = b.instance.instanceResource(Map())
        if (res.getStatus == 200) {
          val resource = res.toBean[CSparkInstanceResource]().head
          returnItem = (resource.totalCores - resource.totalTasks, b)
        }
      } catch {
        case e: Exception =>
      }
      returnItem

    }.sortBy(f => f._1).reverse.headOption.map(f => f._2.meta)

    BackendService.find(backend).map(f => Seq(f))
  }
}

case class CSparkInstanceResource(totalCores: Long, totalTasks: Long, totalUsedMemory: Long, totalMemory: Long)

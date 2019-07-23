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

package tech.mlsql.cluster.service

import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

import com.google.common.cache.{CacheBuilder, CacheLoader}
import net.csdn.annotation.Param
import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.modules.http.RestRequest.Method.{GET, POST}
import net.csdn.modules.transport.HttpTransportService
import net.liftweb.{json => SJSon}
import tech.mlsql.cluster.model.Backend
import tech.mlsql.cluster.service.dispatch.{AllBackendsStrategy, JobNumAwareStrategy, ResourceAwareStrategy}

import scala.collection.JavaConversions._

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
trait BackendService {
  @At(path = Array("/run/script"), types = Array(POST))
  def runScript(@Param("sql") sql: String): HttpTransportService.SResponse

  @At(path = Array("/run/script"), types = Array(POST))
  def runScript(params: Map[String, String]): HttpTransportService.SResponse

  @At(path = Array("/run/sql"), types = Array(POST))
  def runSQL(params: Map[String, String]): HttpTransportService.SResponse

  @At(path = Array("/instance/resource"), types = Array(GET, POST))
  def instanceResource(params: Map[String, String]): HttpTransportService.SResponse
}

case class BackendCache(meta: Backend, instance: BackendService)

case class BackendExecuteMeta(activeTaskNum: Long, totalTaskNum: Long, failTaskNum: Long)

object BackendService {
  val logger = Logger.getLogger("BackendService")
  val backend_meta_key = "backend_meta"
  private val active_task_meta = new java.util.concurrent.ConcurrentHashMap[Backend, AtomicLong]()

  def activeBackend = {
    active_task_meta.map(f => (f._1, f._2.get())).toMap
  }

  def nonActiveBackend = {
    val items = backendMetaCache.get(backend_meta_key).map(f => f.meta).toSet
    items -- active_task_meta.keySet()
  }

  def backends = {
    backendMetaCache.get(backend_meta_key)
  }

  def backendsWithTags(tags: String) = {
    if (tags.isEmpty) backends
    else {
      backends.filter(f => tags.split(",").toSet.intersect(f.meta.getTags.toSet).size > 0)
    }
  }

  def find(backend: Option[Backend]) = {
    backend match {
      case Some(i) => backendMetaCache.get(backend_meta_key).filter(f => f.meta == i).headOption
      case None => None
    }

  }

  def refreshCache = {
    backendMetaCache.refresh(backend_meta_key)
  }

  private val backendMetaCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build(
      new CacheLoader[String, Seq[BackendCache]]() {
        override def load(key: String): Seq[BackendCache] = {
          Backend.items().map { meta =>
            BackendCache(meta, RestService.client(meta.getUrl))
          }
        }
      })


  def execute(f: BackendService => HttpTransportService.SResponse, tags: String, proxyStrategy: String) = {
    var items = backendMetaCache.get(backend_meta_key)

    val chooseProxy = proxyStrategy match {
      case "ResourceAwareStrategy" => new ResourceAwareStrategy(tags)
      case "JobNumAwareStrategy" => new JobNumAwareStrategy(tags)
      case "AllBackendsStrategy" => new AllBackendsStrategy(tags)
      case _ => new ResourceAwareStrategy(tags)
    }
    var backendCache = chooseProxy.invoke(items)

    /*
        If we can not found backend with specified tags, then
        it maybe caused by cache. Clean it and try again
     */
    if (!backendCache.isDefined) {
      refreshCache
      items = backendMetaCache.get(backend_meta_key)
    }

    backendCache = chooseProxy.invoke(items)

    backendCache match {
      case Some(items) =>
        items.seq.map { ins =>
          active_task_meta.putIfAbsent(ins.meta, new AtomicLong())
          val counter = active_task_meta.get(ins.meta)
          try {
            counter.incrementAndGet()
            logger.info(s"Visit backend tagged with ${tags}. Finally we found ${ins.meta.getUrl} with tags:${ins.meta.getTags.mkString(",")}")
            Option(f(ins.instance))
          } finally {
            counter.decrementAndGet()
          }
        }

      case None =>
        logger.info(s"No backened with tags [${tags}] are found")
        Seq(None)
    }
  }

  implicit def mapSResponseToObject(response: HttpTransportService.SResponse): SResponseEnhance = {
    new SResponseEnhance(WowCollections.list(response))
  }
}

class SResponseEnhance(x: java.util.List[HttpTransportService.SResponse]) {

  def toBean[T]()(implicit manifest: Manifest[T]): Option[T] = {
    if (validate) {
      implicit val formats = SJSon.DefaultFormats
      Option(SJSon.parse(x(0).getContent).extract[T])
    } else None
  }

  private def validate = {
    if (x == null || x.isEmpty || x(0).getStatus != 200) {
      false
    }
    else true
  }

  def jsonStr = {
    if (validate) Option(x(0).getContent)
    else None
  }

}
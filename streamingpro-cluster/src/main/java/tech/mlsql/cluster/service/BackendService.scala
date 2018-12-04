package tech.mlsql.cluster.service

import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache.{CacheBuilder, CacheLoader}
import net.csdn.annotation.Param
import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.modules.http.RestRequest.Method.{GET, POST}
import net.csdn.modules.transport.HttpTransportService
import net.liftweb.{json => SJSon}
import tech.mlsql.cluster.model.Backend

import scala.collection.JavaConversions._

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
trait BackendService {
  @At(path = Array("/run/script"), types = Array(GET, POST))
  def runScript(@Param("sql") sql: String): HttpTransportService.SResponse

  @At(path = Array("/run/script"), types = Array(GET, POST))
  def runScript(params: Map[String, String]): HttpTransportService.SResponse

  @At(path = Array("/run/sql"), types = Array(GET, POST))
  def runSQL(params: Map[String, String]): HttpTransportService.SResponse
}

case class BackendCache(meta: Backend, instance: BackendService)

case class BackendExecuteMeta(activeTaskNum: Long, totalTaskNum: Long, failTaskNum: Long)

object BackendService {
  val backend_meta_key = "backend_meta"
  private val active_task_meta = new java.util.concurrent.ConcurrentHashMap[Backend, AtomicLong]()

  def activeBackend = {
    active_task_meta.map(f => (f._1, f._2.get())).toMap
  }

  def nonActiveBackend = {
    val items = backendMetaCache.get(backend_meta_key).map(f => f.meta).toSet
    items -- active_task_meta.keySet()
  }

  def find(backend: Backend) = {
    backendMetaCache.get(backend_meta_key).filter(f => f.meta == backend).head
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


  def execute(f: BackendService => HttpTransportService.SResponse) = {
    val items = backendMetaCache.get(backend_meta_key)

    val chooseProxy = new FirstBackendStrategy()
    val backendCache = chooseProxy.invoke(items)
    val counter = active_task_meta.putIfAbsent(backendCache.meta, new AtomicLong())
    try {
      counter.incrementAndGet()
      f(backendCache.instance)
    } finally {
      counter.decrementAndGet()
    }

  }

  implicit def mapSResponseToObject(response: HttpTransportService.SResponse): SResponseEnhance = {
    new SResponseEnhance(WowCollections.list(response))
  }
}

class SResponseEnhance(x: java.util.List[HttpTransportService.SResponse]) {

  def toBean[T](res: String)(implicit manifest: Manifest[T]): Option[T] = {
    if (validate) {
      implicit val formats = SJSon.DefaultFormats
      Option(SJSon.parse(res).extract[T])
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
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

  @At(path = Array("/instance/resource"), types = Array(GET, POST))
  def instanceResource: HttpTransportService.SResponse
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
    val items = backendMetaCache.get(backend_meta_key)

    val chooseProxy = proxyStrategy match {
      case "FreeCoreBackendStrategy" => new TaskLessBackendStrategy(tags)
      case "TaskLessBackendStrategy" => new FreeCoreBackendStrategy(tags)
      case _ => new TaskLessBackendStrategy(tags)
    }
    val backendCache = chooseProxy.invoke(items)
    backendCache match {
      case Some(ins) =>
        active_task_meta.putIfAbsent(ins.meta, new AtomicLong())
        val counter = active_task_meta.get(ins.meta)
        try {
          counter.incrementAndGet()
          Option(f(ins.instance))
        } finally {
          counter.decrementAndGet()
        }
      case None =>
        logger.info(s"No backened with tags [${tags}] are found")
        None
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
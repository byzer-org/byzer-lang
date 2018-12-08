package tech.mlsql.cluster.service.dispatch

import streaming.log.Logging
import tech.mlsql.cluster.service.BackendService.mapSResponseToObject
import tech.mlsql.cluster.service.{BackendCache, BackendService}

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

class ResourceAwareStrategy(tags: String) extends BackendStrategy {
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

class JobNumAwareStrategy(tags: String) extends BackendStrategy with Logging {
  override def invoke(backends: Seq[BackendCache]): Option[Seq[BackendCache]] = {

    val tagSet = tags.split(",").toSet

    var backends = BackendService.backends
    if (!tags.isEmpty) {
      backends = backends.filter(f => tagSet.intersect(f.meta.getTag.split(",").toSet).size > 0)
    }
    val backend = backends.seq.map { b =>
      logDebug(s"visit backend: ${b.meta.getUrl} tags: ${b.meta.getTag}")
      val res = b.instance.instanceResource(Map())
      val resource = res.toBean[CSparkInstanceResource]().head
      (resource.totalCores - resource.totalTasks, b)
    }.sortBy(f => f._1).reverse.headOption.map(f => f._2.meta)

    BackendService.find(backend).map(f => Seq(f))
  }
}

case class CSparkInstanceResource(totalCores: Long, totalTasks: Long, totalUsedMemory: Long, totalMemory: Long)

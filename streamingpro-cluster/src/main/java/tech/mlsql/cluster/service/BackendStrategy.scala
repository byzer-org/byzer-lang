package tech.mlsql.cluster.service

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
trait BackendStrategy {
  def invoke(backends: Seq[BackendCache]): BackendCache
}

class FirstBackendStrategy extends BackendStrategy {
  override def invoke(backends: Seq[BackendCache]): BackendCache = {
    backends.head
  }
}

class TaskLessBackendStrategy extends BackendStrategy {
  override def invoke(backends: Seq[BackendCache]): BackendCache = {
    val nonActiveBackend = BackendService.nonActiveBackend
    val backend = if (nonActiveBackend.size > 0) {
      nonActiveBackend.head
    } else {
      BackendService.activeBackend.toSeq.sortBy(f => f._2).head._1
    }
    BackendService.find(backend)
  }
}

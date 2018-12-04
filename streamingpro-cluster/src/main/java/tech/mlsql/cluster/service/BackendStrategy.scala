package tech.mlsql.cluster.service

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
trait BackendStrategy {
  def invoke(backends: Seq[BackendCache]): BackendService
}

class FirstBackendStrategy extends BackendStrategy {
  override def invoke(backends: Seq[BackendCache]): BackendService = {
    backends.head.instance
  }
}

package streaming.core.shared

import java.util.concurrent.Executors

import streaming.core.shared.pool.{AnalyserPool, BigObjPool, DicPool}

/**
  * Created by allwefantasy on 21/5/2018.
  */
class SharedObjManager {
  //private[this] val _executor = Executors.newFixedThreadPool(1)
}

object SharedObjManager {
  val analyserPool = new AnalyserPool[Any]()
  val dicPool = new DicPool[Set[String]]()

  def getOrCreate[T](name: String, bigObjPool: BigObjPool[T], func: () => T) = {
    synchronized {
      if (bigObjPool.get(name) == null) {
        bigObjPool.put(name, func())
      }
      bigObjPool.get(name)
    }
  }
}

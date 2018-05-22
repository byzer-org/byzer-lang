package streaming.core.shared.pool

/**
  * Created by allwefantasy on 21/5/2018.
  */
class AnalyserPool[Any] extends BigObjPool[Any] {
  val analyserMap = new java.util.concurrent.ConcurrentHashMap[String, Any]()

  override def size(): Int = analyserMap.size()

  override def get(name: String): Any = analyserMap.get(name)

  override def put(name: String, value: Any): BigObjPool[Any] = {
    analyserMap.put(name, value)
    this
  }

  override def remove(name: String): BigObjPool[Any] = {
    analyserMap.remove(name)
    this
  }
}

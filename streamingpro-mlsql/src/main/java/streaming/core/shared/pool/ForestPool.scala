package streaming.core.shared.pool

/**
  * Created by allwefantasy on 21/5/2018.
  */
class ForestPool[Any] extends BigObjPool[Any] {
  val map = new java.util.concurrent.ConcurrentHashMap[String, Any]()

  override def size(): Int = map.size()

  override def get(name: String): Any = map.get(name)

  override def put(name: String, value: Any): BigObjPool[Any] = {
    map.put(name, value)
    this
  }

  override def remove(name: String): BigObjPool[Any] = {
    map.remove(name)
    this
  }

  def clear = {
    map.clear()
  }
}

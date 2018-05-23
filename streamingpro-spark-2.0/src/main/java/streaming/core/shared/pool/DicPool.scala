package streaming.core.shared.pool

/**
  * Created by allwefantasy on 21/5/2018.
  */
class DicPool[Set[String]] extends BigObjPool[Set[String]] {
  val objMap = new java.util.concurrent.ConcurrentHashMap[String, Set[String]]()

  override def size(): Int = objMap.size()

  override def get(name: String): Set[String] = objMap.get(name)

  override def put(name: String, value: Set[String]): BigObjPool[Set[String]] = {
    objMap.put(name, value)
    this
  }

  override def remove(name: String): BigObjPool[Set[String]] = {
    objMap.remove(name)
    this
  }
}

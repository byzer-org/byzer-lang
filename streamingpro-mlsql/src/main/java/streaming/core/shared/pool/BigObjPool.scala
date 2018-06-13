package streaming.core.shared.pool

/**
  * Created by allwefantasy on 21/5/2018.
  */
trait BigObjPool[T] {
  def size(): Int

  def get(name: String): T

  def put(name: String, value: T): BigObjPool[T]

  def remove(name: String): BigObjPool[T]
}

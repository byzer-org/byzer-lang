package tech.mlsql.runtime

import com.fasterxml.jackson.annotation.JsonIgnore
import tech.mlsql.app.CustomController
import tech.mlsql.runtime.kvstore.{InMemoryStore, KVIndex, KVStore}

/**
 * 6/11/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class AppRuntimeStore(val store: KVStore, val listener: Option[AppSRuntimeListener] = None) extends ControllerRuntimeStore {

}

trait ControllerRuntimeStore {
  self: AppRuntimeStore =>
  def registerController(name: String, className: String) = {
    store.write(CustomClassItemWrapper(CustomClassItem(name, className)))
  }

  def removeController(name: String) = {
    store.delete(classOf[CustomClassItemWrapper], name)
  }

  def getController(name: String): Option[CustomClassItemWrapper] = {
    try {
      Some(store.read(classOf[CustomClassItemWrapper], name))
    } catch {
      case e: NoSuchElementException =>
        None
      case e: Exception => throw e
    }

  }

}

object AppRuntimeStore {
  private val _store = new InMemoryStore()
  val store = new AppRuntimeStore(_store)
}

class Jack extends CustomController {
  override def run(params: Map[String, String]): String = {
    "[]"
  }
}

class AppSRuntimeListener {}

case class CustomClassItemWrapper(customClassItem: CustomClassItem) {
  @JsonIgnore
  @KVIndex
  def id = customClassItem.name
}

case class CustomClassItem(@KVIndex name: String, className: String)

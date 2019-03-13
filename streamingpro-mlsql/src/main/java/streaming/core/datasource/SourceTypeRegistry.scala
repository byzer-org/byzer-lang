package streaming.core.datasource

import scala.collection.JavaConverters._
import scala.collection.mutable

object SourceTypeRegistry {
  private val registry = new java.util.concurrent.ConcurrentHashMap[String, mutable.Set[SourceInfo]]()

  def register(name: String, obj: SourceInfo) = {

    if (!registry.containsKey(name)) {
      registry.put(name, new mutable.HashSet[SourceInfo]())
    }
    registry.get(name) += obj
  }

  def sources = {
    registry.keys().asScala.toSeq
  }
}
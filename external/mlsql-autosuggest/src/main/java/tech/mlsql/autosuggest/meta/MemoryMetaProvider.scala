package tech.mlsql.autosuggest.meta

import scala.collection.JavaConverters._

/**
 * 15/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MemoryMetaProvider extends MetaProvider {
  private val cache = new java.util.concurrent.ConcurrentHashMap[MetaTableKey, MetaTable]()

  override def search(key: MetaTableKey): Option[MetaTable] = {
    if (cache.containsKey(key)) Option(cache.get(key)) else None
  }

  override def list: List[MetaTable] = {
    cache.values().asScala.toList
  }

  def register(key: MetaTableKey, value: MetaTable) = {
    cache.put(key, value)
    this
  }

  def unRegister(key: MetaTableKey) = {
    cache.remove(key)
    this
  }

  def clear = {
    cache.clear()
    this
  }

}

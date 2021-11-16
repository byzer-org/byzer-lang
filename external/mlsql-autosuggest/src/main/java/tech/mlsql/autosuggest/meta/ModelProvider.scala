package tech.mlsql.autosuggest.meta

/**
 * 15/11/2021 hellozepp(lisheng.zhanglin@163.com)
 */
class ModelProvider extends MetaProvider {
  private val cache = scala.collection.mutable.HashMap[String, MetaTable]()

  override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
    cache.get(key.table)
  }

  def register(name: String, metaTable: MetaTable): ModelProvider = {
    cache += (name -> metaTable)
    this
  }

  override def list(extra: Map[String, String] = Map()): List[MetaTable] = cache.values.toList
}

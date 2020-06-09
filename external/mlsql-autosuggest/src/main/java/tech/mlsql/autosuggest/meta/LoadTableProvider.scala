package tech.mlsql.autosuggest.meta

/**
 * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class LoadTableProvider extends MetaProvider {
  private val cache = scala.collection.mutable.HashMap[String, MetaTable]()

  override def search(key: MetaTableKey): Option[MetaTable] = {
    cache.get(key.table)
  }

  def register(name: String, metaTable: MetaTable) = {
    cache += (name -> metaTable)
    this
  }

  override def list: List[MetaTable] = cache.values.toList
}

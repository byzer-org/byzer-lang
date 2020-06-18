package tech.mlsql.autosuggest.meta

/**
 * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class StatementTempTableProvider extends MetaProvider {
  private val cache = scala.collection.mutable.HashMap[String, MetaTable]()

  override def search(key: MetaTableKey,extra: Map[String, String] = Map()): Option[MetaTable] = {
    cache.get(key.table)
  }

  def register(name: String, metaTable: MetaTable) = {
    cache += (name -> metaTable)
    this
  }

  override def list(extra: Map[String, String] = Map()): List[MetaTable] = cache.values.toList
}

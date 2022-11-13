package tech.mlsql.autosuggest.meta

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class LayeredMetaProvider(tempTableProvider: StatementTempTableProvider, runtimeMetaProvider: RuntimeMetaProvider, userDefinedProvider: MetaProvider) extends MetaProvider {
  def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
    runtimeMetaProvider.search(key, extra).orElse {
      tempTableProvider.search(key).orElse {
        userDefinedProvider.search(key)
      }
    }
  }

  def list(extra: Map[String, String] = Map()): List[MetaTable] = {
    tempTableProvider.list(extra) ++ runtimeMetaProvider.list(extra) ++ userDefinedProvider.list(extra)
  }
}

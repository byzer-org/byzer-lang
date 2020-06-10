package tech.mlsql.autosuggest.meta

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class LayeredMetaProvider(tempTableProvider: StatementTempTableProvider, userDefinedProvider: MetaProvider) extends MetaProvider {
  def search(key: MetaTableKey): Option[MetaTable] = {
    tempTableProvider.search(key).orElse {
      userDefinedProvider.search(key)
    }
  }

  def list: List[MetaTable] = {
    tempTableProvider.list ++ userDefinedProvider.list
  }
}

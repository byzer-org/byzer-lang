package tech.mlsql.autosuggest.meta

/**
 * Function should also be table and at the same time
 * the columns are treated as parameters.
 * Without MetaProvider supporting, we can not
 * suggest column and functions .
 *
 * If the search returns None, this means it's not a table
 * or the table we are searching is not exists.
 */
trait MetaProvider {
  def search(key: MetaTableKey,extra: Map[String, String] = Map()): Option[MetaTable]

  def list(extra: Map[String, String] = Map()): List[MetaTable]
}



package tech.mlsql.autosuggest.statement

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait SuggesterRegister {
  def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister
}

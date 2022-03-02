package tech.mlsql.datasource.helper.rest

/**
 * 3/12/2021 WilliamZhu(allwefantasy@gmail.com)
 */
object PageStrategyDispatcher {
  def get(params: Map[String, String]): PageStrategy = {
    params("config.page.values").trim.toLowerCase match {
      case s if s.startsWith("auto-increment") || s.startsWith("autoIncrement") =>
        new AutoIncrementPageStrategy(params)
      case s if s.startsWith("offset") =>
        new OffsetPageStrategy(params)
      case _ => new DefaultPageStrategy(params)
    }
  }
}

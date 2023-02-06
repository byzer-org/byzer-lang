package streaming.core.datasource.impl

import streaming.core.datasource._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
 * 2023-01-29 lin.zhang
 */
class MLSQLParquet(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "parquet"

  override def shortFormat: String = fullFormat

}

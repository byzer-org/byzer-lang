package tech.mlsql.datasource.impl

import org.apache.spark.sql.SparkSession
import streaming.core.datasource.{DataSourceRegistry, MLSQLBaseFileSource, MLSQLDataSourceKey, MLSQLSparkDataSourceType}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
 * 31/8/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLBinaryFile(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }


  override def fullFormat: String = "org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat"

  override def shortFormat: String = "binaryFile"

}

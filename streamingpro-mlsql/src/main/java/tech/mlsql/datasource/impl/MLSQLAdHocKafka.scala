package tech.mlsql.datasource.impl

import streaming.core.datasource.impl.MLSQLKafka
import streaming.dsl.mmlib.algs.param.BaseParams

/**
  * 2019-06-25 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLAdHocKafka(override val uid: String) extends MLSQLKafka{
  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "org.apache.spark.sql.kafka010.AdHocKafkaSourceProvider"

  override def shortFormat: String = "adHocKafka"
}

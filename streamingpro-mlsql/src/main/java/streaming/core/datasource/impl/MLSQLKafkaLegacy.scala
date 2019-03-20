package streaming.core.datasource.impl

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLKafkaLegacy extends MLSQLKafka {
  override def fullFormat: String = "com.hortonworks.spark.sql.kafka08"

  override def shortFormat: String = "kafka8"

}

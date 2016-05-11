package org.apache.spark

import _root_.streaming.core.strategy.platform.RuntimeOperator
import org.apache.spark.sql.SQLContext

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkRuntimeOperator(sparkContext: SparkContext) extends RuntimeOperator {
  def createTable(resource: String, tableName: String, esOptions: Map[String, String]) = {
    //val esOptions = Map("es.nodes"->"192.168.1.2,192.168.1.3", "es.scroll.size"->"1000", "es.field.read.as.array.include"->"SampleField")
    val esDF = SQLContext.getOrCreate(sparkContext).read.format("org.elasticsearch.spark.sql").options(esOptions).load(resource)
    esDF.registerTempTable(tableName)
  }

  def runSQL(sql: String) = {
    val df = SQLContext.getOrCreate(sparkContext).sql(sql)
    df.toJSON.collect()
  }
}

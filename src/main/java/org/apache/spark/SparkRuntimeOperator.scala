package org.apache.spark

import java.util.{Map => JMap}

import _root_.streaming.common.SQLContextHolder
import _root_.streaming.core.strategy.platform.RuntimeOperator
import org.apache.spark.sql.SQLContext

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkRuntimeOperator(params: JMap[Any, Any], sparkContext: SparkContext) extends RuntimeOperator {

  def createTable(resource: String, tableName: String, dataSourceOptions: Map[String, String]) = {
    //val esOptions = Map("es.nodes"->"192.168.1.2,192.168.1.3", "es.scroll.size"->"1000", "es.field.read.as.array.include"->"SampleField")
    //"org.elasticsearch.spark.sql"
    val loader_clzz = dataSourceOptions("loader_clzz." + tableName)
    val df = SQLContextHolder.getOrCreate.getOrCreate().read.format(loader_clzz).options(dataSourceOptions - loader_clzz).load(resource)
    df.registerTempTable(tableName)
  }

  def runSQL(sql: String) = {
    val df = SQLContextHolder.getOrCreate.getOrCreate().sql(sql)
    df.toJSON.collect()
  }
}

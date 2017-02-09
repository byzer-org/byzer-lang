package org.apache.spark

import java.util.{Map => JMap}

import _root_.streaming.common.SQLContextHolder
import _root_.streaming.core.strategy.platform.RuntimeOperator

/**
 * 5/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SparkRuntimeOperator(params: JMap[Any, Any], sparkContext: SparkContext) extends RuntimeOperator {

  def createTable(resource: String, tableName: String, dataSourceOptions: Map[String, String]) = {
    //val esOptions = Map("es.nodes"->"192.168.1.2,192.168.1.3", "es.scroll.size"->"1000", "es.field.read.as.array.include"->"SampleField")
    //"org.elasticsearch.spark.sql"
    var loader_clzz = dataSourceOptions("loader_clzz." + tableName)



    val options = if (loader_clzz == "carbondata") {
      dataSourceOptions + ("tableName" -> resource)
    } else {
      dataSourceOptions + ("path" -> resource)
    }

    if (loader_clzz == "carbondata") {
      loader_clzz = "org.apache.spark.sql.CarbonSource"
    }

    val df = SQLContextHolder.getOrCreate.getOrCreate().
      read.format(loader_clzz).
      options(options - loader_clzz).
      load()

    df.registerTempTable(tableName)
  }

  def runSQL(sql: String) = {
    val df = SQLContextHolder.getOrCreate.getOrCreate().sql(sql)
    df.toJSON.collect()
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.util.{Map => JMap}

import org.apache.spark.sql.SparkSession

/**
  * Created by allwefantasy on 30/3/2017.
  */
class SparkRuntimeOperator(sparkSession: SparkSession) {

  def createTable(resource: String, tableName: String, dataSourceOptions: Map[String, String]): Unit = {

    //val esOptions = Map("es.nodes"->"192.168.1.2,192.168.1.3", "es.scroll.size"->"1000", "es.field.read.as.array.include"->"SampleField")
    //"org.elasticsearch.spark.sql"
    var loader_clzz = dataSourceOptions("loader_clzz." + tableName)



    val options = if (loader_clzz == "carbondata") {
      dataSourceOptions + ("tableName" -> resource)
    } else {
      if (dataSourceOptions.contains("path") || dataSourceOptions.contains("paths")) {
        dataSourceOptions
      } else {
        dataSourceOptions + ("path" -> resource)
      }

    }

    if (loader_clzz == "carbondata") {
      loader_clzz = "org.apache.spark.sql.CarbonSource"
    }

    val df = sparkSession.
      read.format(loader_clzz).
      options(options - loader_clzz - ("loader_clzz." + tableName)).
      load()

    df.createOrReplaceTempView(tableName)
  }

  def runSQL(sql: String) = {
    val df = sparkSession.sql(sql)
    df.toJSON.collect()
  }
}

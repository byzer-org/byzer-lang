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

package tech.mlsql.test.dsl

import java.util

import org.apache.spark.MLSQLConf
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.{BasicMLSQLConfig, SpecFunctions}

import scala.collection.JavaConversions._

/**
  * 2018-11-30 WilliamZhu(allwefantasy@gmail.com)
  */
class ConfigSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "MLSQLConf" should "get streaming.platform right" in {
    val params = new util.HashMap[Any, Any]()
    params.put("streaming.platform", "spark")

    val configReader = MLSQLConf.createConfigReader(params.map(f => (f._1.toString, f._2.toString)))
    assume(MLSQLConf.MLSQL_PLATFORM.readFrom(configReader).get == "spark")
  }

  "MLSQLConf" should "get default value right" in {
    val params = new util.HashMap[Any, Any]()
    val configReader = MLSQLConf.createConfigReader(params.map(f => (f._1.toString, f._2.toString)))
    assume(MLSQLConf.MLSQL_NAME.readFrom(configReader) == "mlsql")
  }

  "MLSQLConf" should "auto convert type right" in {
    val params = new util.HashMap[Any, Any]()
    params.put(MLSQLConf.MLSQL_SPARK_SERVICE.key, "true")
    val configReader = MLSQLConf.createConfigReader(params.map(f => (f._1.toString, f._2.toString)))
    assume(MLSQLConf.MLSQL_SPARK_SERVICE.readFrom(configReader))
  }
}

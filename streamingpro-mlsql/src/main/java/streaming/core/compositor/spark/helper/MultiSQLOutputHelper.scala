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

package streaming.core.compositor.spark.helper

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by allwefantasy on 16/7/2017.
  */
object MultiSQLOutputHelper {
  def output(_cfg: Map[String, String], spark: SparkSession) = {
    val tableName = _cfg("inputTableName")
    val outputFileNum = _cfg.getOrElse("outputFileNum", "-1").toInt
    val partitionBy = _cfg.getOrElse("partitionBy", "")
    var newTableDF = spark.table(tableName)

    if (outputFileNum != -1) {
      newTableDF = newTableDF.repartition(outputFileNum)
    }

    val _resource = _cfg("path")
    val options = _cfg - "path" - "mode" - "format"
    val dbtable = if (options.contains("dbtable")) options("dbtable") else _resource
    val mode = _cfg.getOrElse("mode", "ErrorIfExists")
    val format = _cfg("format")

    if (format == "console") {
      newTableDF.show(_cfg.getOrElse("showNum", "100").toInt)
    } else {

      var tempDf = if (!partitionBy.isEmpty) {
        newTableDF.write.partitionBy(partitionBy.split(","): _*)
      } else {
        newTableDF.write
      }

      tempDf = tempDf.options(options).mode(SaveMode.valueOf(mode)).format(format)

      if (_resource == "-" || _resource.isEmpty) {
        tempDf.save()
      } else tempDf.save(_resource)
    }
  }
}

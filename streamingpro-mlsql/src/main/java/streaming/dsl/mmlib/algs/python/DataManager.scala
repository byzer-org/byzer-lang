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

package streaming.dsl.mmlib.algs.python

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.util.ObjPickle
import streaming.dsl.mmlib.algs.SQLPythonFunc
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging

class DataManager(df: DataFrame, path: String, params: Map[String, String]) extends Logging with WowLog {

  def enableDataLocal = {
    params.getOrElse("enableDataLocal", "true").toBoolean
  }

  def saveDataToHDFS = {
    var dataHDFSPath = ""
    // persist training data to HDFS
    if (enableDataLocal) {
      val dataLocalizeConfig = DataLocalizeConfig.buildFromParams(params)
      dataHDFSPath = SQLPythonFunc.getAlgTmpPath(path) + "/data"

      val newDF = if (dataLocalizeConfig.dataLocalFileNum > -1) {
        df.repartition(dataLocalizeConfig.dataLocalFileNum)
      } else df
      newDF.write.format(dataLocalizeConfig.dataLocalFormat).mode(SaveMode.Overwrite).save(dataHDFSPath)
    }
    dataHDFSPath
  }

  def broadCastValidateTable = {
    val schema = df.schema
    var rows = Array[Array[Byte]]()
    //目前我们只支持同一个测试集
    if (params.contains("validateTable") || params.contains("evaluateTable")) {
      val validateTable = params.getOrElse("validateTable", params.getOrElse("evaluateTable", ""))
      rows = df.sparkSession.table(validateTable).rdd.mapPartitions { iter =>
        ObjPickle.pickle(iter, schema)
      }.collect()
    }
    df.sparkSession.sparkContext.broadcast(rows)
  }

}

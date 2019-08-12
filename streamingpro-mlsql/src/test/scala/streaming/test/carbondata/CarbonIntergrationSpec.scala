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

package streaming.test.carbondata

import net.sf.json.JSONObject
import org.apache.spark.streaming.BasicSparkOperation
import org.apache.spark.{CarbonCoreVersion, SparkCoreVersion}
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge
import tech.mlsql.common.utils.shell.ShellCommand

/**
  * Created by allwefantasy on 12/9/2018.
  */
class CarbonIntergrationSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  val checkCarbonDataCoreCompatibility = CarbonCoreVersion.coreCompatibility(SparkCoreVersion.version, SparkCoreVersion.exactVersion)

  "script-support-drop" should "work fine" in {

    ShellCommand.execCmd("rm -rf /tmp/carbondata/store")
    ShellCommand.execCmd("rm -rf /tmp/carbondata/meta")

    withBatchContext(setupBatchContext(batchParamsWithCarbondata, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      try {
        if (checkCarbonDataCoreCompatibility) {
          val tableName = "visit_carbon5"
          var sql =
            """
              |select "1" as a
              |as mtf1;
              |
              |save overwrite mtf1
              |as carbondata.`-`
              |options mode="overwrite"
              |and tableName="${tableName}"
              |and implClass="org.apache.spark.sql.CarbonSource";
            """.stripMargin

          var sq = createSSEL

          ScriptSQLExec.parse(TemplateMerge.merge(sql, Map("tableName" -> tableName)), sq)
          val res = spark.sql("select * from " + tableName).toJSON.collect()
          val keyRes = JSONObject.fromObject(res(0)).getString("a")
          assume(keyRes == "1")

          sql =
            """
              |drop table ${tableName};
            """.stripMargin

          sq = createSSEL
          ScriptSQLExec.parse(TemplateMerge.merge(sql, Map("tableName" -> tableName)), sq)
          try {
            spark.sql("select * from " + tableName).toJSON.collect()
            assume(0 == 1)
          } catch {
            case e: Exception =>
          }
        }
      } finally {

      }
    }
  }
  "carbondata save" should "work fine" in {

    ShellCommand.execCmd("rm -rf /tmp/carbondata/store")
    ShellCommand.execCmd("rm -rf /tmp/carbondata/meta")

    withBatchContext(setupBatchContext(batchParamsWithCarbondata, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      if (checkCarbonDataCoreCompatibility) {
        try {
          //执行sql
          implicit val spark = runtime.sparkSession
          var sq = createSSEL
          var tableName = "visit_carbon3"

          dropTables(Seq(tableName))

          ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("mlsql-carbondata"), Map("tableName" -> tableName)), sq)
          Thread.sleep(1000)
          var res = spark.sql("select * from " + tableName).toJSON.collect()
          var keyRes = JSONObject.fromObject(res(0)).getString("a")
          assume(keyRes == "1")

          dropTables(Seq(tableName))

          sq = createSSEL
          tableName = "visit_carbon4"

          dropTables(Seq(tableName))

          ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("mlsql-carbondata-without-option"), Map("tableName" -> tableName)), sq)
          Thread.sleep(1000)
          res = spark.sql("select * from " + tableName).toJSON.collect()
          keyRes = JSONObject.fromObject(res(0)).getString("a")
          assume(keyRes == "1")
          dropTables(Seq(tableName))
        } finally {
        }

      }
    }
  }

}

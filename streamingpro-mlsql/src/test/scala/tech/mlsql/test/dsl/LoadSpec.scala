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

import java.io.File

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, NotToRunTag, SpecFunctions}
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.shell.ShellCommand
import tech.mlsql.dsl.processor.GrammarProcessListener

/**
  * Created by allwefantasy on 26/4/2018.
  */
class LoadSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "load with/without columns" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) {
      runtime: SparkRuntime =>
        //执行sql
        implicit val spark = runtime.sparkSession

        val ssel = createSSEL
        val mlsql =
          """
            |select "a" a,"b" b,"c" c as t1;
            |save overwrite t1 as parquet.`/tmp/jack`;
            |load parquet.`/tmp/jack` options withColumns="a,b" as jackm;
            |select *  from jackm as output;
          """.stripMargin
        ScriptSQLExec.parse(mlsql, ssel)
        assume(spark.sql("select * from output").schema.fieldNames.mkString(",") == "a,b")

        val mlsql2 =
          """
            |select "a" a,"b" b,"c" c as t1;
            |save overwrite t1 as parquet.`/tmp/jack`;
            |load parquet.`/tmp/jack` options withoutColumns="a,b" as jackm;
            |select *  from jackm as output;
          """.stripMargin
        ScriptSQLExec.parse(mlsql2, ssel)
        assume(spark.sql("select * from output").schema.fieldNames.mkString(",") == "c")

    }
  }
}


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

package streaming.test.datasource

import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.log.Logging

/**
  * Created by latincross on 12/27/2018.
  */
class HDFSWholeBinaryFileSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {

  "load hdfs whole binary file" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      var sq = createSSEL(spark, "", "-")

      ScriptSQLExec.parse(
        s"""
           |
           |select 'abc' str as testWholeBinary;
           |
           |save overwrite testWholeBinary as csv.`/tmp/wbtest`;
           |
           |load wholeBinary.`/tmp/wbtest` as wbTable;
           |
         """.stripMargin, sq)
      val df = spark.sql("select path, content from wbTable")
      val schema = df.schema
      assume(schema.size == 2 && schema.fields(0).dataType.getClass == StringType.getClass && schema.fields(1).dataType.getClass == BinaryType.getClass)
      val item = df.collect().last
      val path = item.getString(0);
      val ctxAsStr = new String(item.getAs[Array[Byte]](1))
      assume(path.endsWith("csv") && ctxAsStr.trim == "abc")
    }
  }


  "save hdfs whole binary file" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      var sq = createSSEL(spark, "", "-")

      ScriptSQLExec.parse(
        s"""
           |
           |select 'twbFile' path, binary('abc') content as testWholeBinary;
           |
           |save overwrite testWholeBinary as wholeBinary.`/tmp/wbtest2`;
           |
           |load wholeBinary.`/tmp/wbtest2` as wbTable;
           |
         """.stripMargin, sq)
      val df = spark.sql("select path, content from wbTable")
      val schema = df.schema
      assume(schema.size == 2 && schema.fields(0).dataType.getClass == StringType.getClass && schema.fields(1).dataType.getClass == BinaryType.getClass)
      val item = df.collect().last
      val path = item.getString(0);
      val ctxAsStr = new String(item.getAs[Array[Byte]](1))
      assume(path.endsWith("twbFile") && ctxAsStr.trim == "abc")
    }
  }
}

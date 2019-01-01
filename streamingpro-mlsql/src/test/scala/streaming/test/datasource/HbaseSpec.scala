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

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.log.Logging

/**
  * Created by latincross on 12/27/2018.
  */
class HbaseSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
  "load hbase" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |select 'a' as id, 'c' as ck, 1 as jk,1552419324001 as ck_ts as test ;
           |
           |save overwrite test
           |as hbase.`test_ns:test_tb`
           |options  rowkey="id"
           |and family="cf"
           |and `field.type.ck`="StringType"
           |and `field.type.jk`="StringType"
           |and zk="hbase-docker:2181";
           |
           |load hbase.`test_ns:test_tb`
           |options zk="hbase-docker:2181"
           |and family="cf"
           |as output1;
           |
           |connect hbase where
           |    namespace="test_ns"
           |and family="cf"
           |and zk="hbase-docker:2181" as hbase_instance;
           |
           |load hbase.`hbase_instance:test_tb`
           |as output2;
         """.stripMargin, sq)
      assume(spark.sql("select rowkey from output1").collect().last.get(0) == "a")
      assume(spark.sql("select rowkey from output2").collect().last.get(0) == "a")
    }
  }

  val server = new streaming.test.servers.HbaseServer("1.2")

  override protected def beforeAll(): Unit = {
    server.startServer
    server.exec("hbase", "exec hbase shell <<EOF \n create_namespace 'test_ns' \n create 'test_ns:test_tb', 'cf' \nEOF\n")
  }

  override protected def afterAll(): Unit = {
    server.stopServer
  }
}

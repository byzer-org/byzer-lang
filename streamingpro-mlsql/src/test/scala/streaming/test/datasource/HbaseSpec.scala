///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package streaming.test.datasource
//
//import org.apache.spark.streaming.BasicSparkOperation
//import org.scalatest.BeforeAndAfterAll
//import streaming.core.strategy.platform.SparkRuntime
//import streaming.core.{BasicMLSQLConfig, SpecFunctions}
//import streaming.dsl.ScriptSQLExec
//import streaming.log.Logging
//
///**
//  * Created by latincross on 12/27/2018.
//  */
//class HbaseSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
//  "load hbase" should "work fine" in {
//
//    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
//      //执行sql
//      implicit val spark = runtime.sparkSession
//
//      var sq = createSSEL
//      ScriptSQLExec.parse(
//        s"""
//           |
//           |connect hbase where `zk`="127.0.0.1:2181"
//           |and `family`="cf" as hbase1;
//           |
//           |load hbase.`hbase1:mlsql_example`
//           |as mlsql_example;
//           |
//           |select * from mlsql_example as show_data;
//           |
//           |
//           |select '2' as rowkey, 'insert test data' as name as insert_table;
//           |
//           |save insert_table as hbase.`hbase1:mlsql_example`;
//           |
//         """.stripMargin, sq)
//      assume(spark.sql("select * from mlsql_example where rowkey='2' ").collect().last.get(0) == "2")
//
//      ScriptSQLExec.parse(
//        s"""
//           |
//           |connect hbase where `zk`="127.0.0.1:2181"
//           |and `tsSuffix`="_ts"
//           |and `family`="cf" as hbase_conn;
//           |
//           |select 'a' as id, 1 as ck, 1552419324001 as ck_ts as test ;
//           |save overwrite test
//           |as hbase.`hbase_conn:mlsql_example`
//           |options  rowkey="id";
//           |
//           |load hbase.`hbase_conn:mlsql_example`
//           |options `field.type.ck`="IntegerType"
//           |as testhbase;
//         """.stripMargin, sq)
//      assume(spark.sql("select get_json_object(content,'$.cf:ck_ts') ts from testhbase where rowkey='a' ")
//        .collect().last.get(0) == "1552419324001")
//    }
//  }
//
//  val server = new streaming.test.servers.HbaseServer("1.2")
//
//  override protected def beforeAll(): Unit = {
//    server.startServer
//    server.exec("hbase", s"""echo \"create 'mlsql_example','cf'\"| hbase shell""")
//    server.exec("hbase", s"""echo \"put 'mlsql_example','1','cf:name','this is a test data'\"| hbase shell""")
//  }
//
//  override protected def afterAll(): Unit = {
//    server.stopServer
//  }
//}

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

package streaming.test.mysql

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.log.Logging

/**
  * Created by allwefantasy on 12/9/2018.
  */
class MySQLSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {

  val connect_stat =
    s"""
       |set user="root";
       |connect jdbc where
       |url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
       |and driver="com.mysql.jdbc.Driver"
       |and user="$${user}"
       |and password="${password}"
       |as tableau;
    """.stripMargin

  "save mysql with update" should "work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      //注册表连接
      var sq = createSSEL
      ScriptSQLExec.parse(connect_stat, sq)

      sq = createSSEL
      ScriptSQLExec.parse("select \"a\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      jdbc("drop table if exists tod_boss_dashboard_sheet_1", connect_stat)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save append tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_1`
           |options truncate="true"
           |and idCol="a,b"
           |and createTableColumnTypes="a VARCHAR(128),b VARCHAR(128)";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_1` as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save append tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_1`
           |options idCol="a,b";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_1` as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

      sq = createSSEL
      ScriptSQLExec.parse("select \"k\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save append tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_1`
           |;
           |load jdbc.`tableau.tod_boss_dashboard_sheet_1` where enableCacheToHDFS="false" as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 2)
    }
  }

  "save mysql with default" should "work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      //注册表连接
      var sq = createSSEL
      ScriptSQLExec.parse(connect_stat, sq)

      sq = createSSEL
      ScriptSQLExec.parse("select \"a\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save overwrite tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2`
           |options truncate="false";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` where enableCacheToHDFS="false" as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save append tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2`
           |;
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` where enableCacheToHDFS="false" as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 2)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save overwrite tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2` options truncate="true"
           |;
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` where enableCacheToHDFS="false" as  tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

    }
  }

  "load with cache" should "work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive ++ Array("-spark.mlsql.enable.datasource.mysql.cache", "true"), "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //注册表连接
      var sq = createSSEL
      ScriptSQLExec.parse(connect_stat, sq)

      sq = createSSEL
      ScriptSQLExec.parse("select \"a\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      sq = createSSEL

      val baseHome = "/tmp/william/tmp/_jdbc_cache_"
      val mysql_wow_tod_boss_dashboard_sheet_2 = s"${baseHome}/mysql_wow_tod_boss_dashboard_sheet_2"
      HDFSOperator.deleteDir(baseHome)
      ScriptSQLExec.parse(
        s"""
           |save overwrite tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2`
           |options truncate="false";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` as tbs;
         """.stripMargin, sq)

      assume(HDFSOperator.fileExists(mysql_wow_tod_boss_dashboard_sheet_2 + "/data"))

      HDFSOperator.deleteDir(baseHome)

      val threads = (0 until 10).map { i =>
        new Thread(new Runnable {
          override def run(): Unit = {
            val sq = createSSEL
            ScriptSQLExec.parse(
              s"""
                 |load jdbc.`tableau.tod_boss_dashboard_sheet_2` as tbs;
         """.stripMargin, sq)
          }

        })
      }
      threads.foreach(f => f.start())
      threads.foreach(f => f.join())
      assume(spark.read.parquet(s"${mysql_wow_tod_boss_dashboard_sheet_2}/data").count() == 1)

    }
  }

  "SQLJDBC" should "work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      //注册表连接
      var sq = createSSEL
      ScriptSQLExec.parse(connect_stat, sq)

      sq = createSSEL
      ScriptSQLExec.parse("select \"a\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save overwrite tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2`
           |options truncate="false";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` where enableCacheToHDFS="false" as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |select 1 as t as NoneTable;
           |run NoneTable as JDBC.`tableau.tod_boss_dashboard_sheet_2`
           |where `driver-statement-query`="select * from tod_boss_dashboard_sheet_2"
           |and sqlMode="query"
           |as jack;
           |
         """.stripMargin, sq)
      assume(spark.sql("select * from jack").collect().size == 1)

      ScriptSQLExec.parse(
        s"""
           |select 1 as t as NoneTable;
           |run NoneTable as JDBC.``
           |where `driver-statement-query`="select * from tod_boss_dashboard_sheet_2"
           |and sqlMode="query"
           |and url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
           |and driver="com.mysql.jdbc.Driver"
           |and user="root"
           |and password="${password}"
           |as jack;
           |
         """.stripMargin, sq)
      assume(spark.sql("select * from jack").collect().size == 1)

    }
  }

  "MLSQLDirectJDBC" should "work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      //注册表连接
      var sq = createSSEL
      ScriptSQLExec.parse(connect_stat, sq)

      sq = createSSEL
      ScriptSQLExec.parse("select \"a\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save overwrite tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2`
           |options truncate="false";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` where directQuery='''
           |select * from tod_boss_dashboard_sheet_2 where a = "b"
           |''' as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 0)


    }
  }

  val server = new streaming.test.servers.MySQLServer("5.7")

  override protected def beforeAll(): Unit = {
    server.startServer
    server.exec("mysql", "exec mysql -uroot -pmlsql --protocol=tcp -e 'create database wow'")
  }

  override protected def afterAll(): Unit = {
    server.stopServer
  }

}

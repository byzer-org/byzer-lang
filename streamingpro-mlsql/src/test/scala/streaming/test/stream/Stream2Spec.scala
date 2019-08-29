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

package streaming.test.stream

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.shell.ShellCommand

class Stream2Spec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll {

  val topic_name = "test_cool"

  "kafka" should "work fine on spark" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      ShellCommand.execCmd("rm -rf /tmp/william/tmp/cpl3")
      new Thread(new Runnable {
        override def run(): Unit = {
          var count = 20
          while (count > 0) {
            val ssel = createSSEL
            try {
              ScriptSQLExec.parse(
                """
                  |select "a" as value as tmp_table1;
                  |save append tmp_table1
                  |as kafka.`test_cool` where kafka.bootstrap.servers="127.0.0.1:9092";
                """.stripMargin, ssel)
            } catch {
              case e: Exception =>
                print(e.getMessage)
            }
            Thread.sleep(1000)
            count -= 1
          }

        }
      }).start()

      val ssel = createSSEL
      ScriptSQLExec.parse(
        """
          |set streamName="streamExample";
          |load kafka.`test_cool` options kafka.bootstrap.servers="127.0.0.1:9092"
          |as table1;
          |
          |save append table1
          |as console.``
          |options mode="Append"
          |and duration="2"
          |and checkpointLocation="/tmp/cpl3";
        """.stripMargin, ssel)
      Thread.sleep(1000 * 30)
      assume(spark.streams.active.size > 0)
      spark.streams.active.foreach(f => f.stop())

    }
  }

  "mysql-sinke" should "work fine " in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      ShellCommand.execCmd("rm -rf /tmp/william/tmp/cpl3")

      val ssel = createSSEL

      val connect_stat =
        s"""
           |set user="root";
           |connect jdbc where
           |url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
           |and driver="com.mysql.jdbc.Driver"
           |and user="$${user}"
           |and password="${password}"
           |as wow_proxy;
    """.stripMargin

      ScriptSQLExec.parse(connect_stat, ssel)

      new Thread(new Runnable {
        override def run(): Unit = {
          var count = 20
          while (count > 0) {
            val ssel = createSSEL
            try {
              ScriptSQLExec.parse(
                """
                  |select "a" as value as tmp_table1;
                  |save append tmp_table1
                  |as kafka.`test_cool` where kafka.bootstrap.servers="127.0.0.1:9092";
                """.stripMargin, ssel)
            } catch {
              case e: Exception =>
                print(e.getMessage)
            }
            Thread.sleep(1000)
            count -= 1
          }

        }
      }).start()

      ScriptSQLExec.parse(
        """
          |set streamName="streamExample";
          |load kafka.`test_cool` options kafka.bootstrap.servers="127.0.0.1:9092"
          |as table1;
          |
          |select cast(value as string) as k, 1 as c from table1 as resultDataTable;
          |
          |save append resultDataTable
          |as streamJDBC.`wow_proxy.test1`
          |options mode="Append"
          |and `driver-statement-0`='''
          |     CREATE TABLE IF NOT EXISTS test1
          |       (
          |          id   int(11) NOT NULL AUTO_INCREMENT,
          |          k    varchar(255) DEFAULT NULL,
          |          c    int(11),
          |          PRIMARY KEY (id)
          |       ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
          |'''
          |and `statement-0`="insert into test1(k,c) values(?,?)"
          |and duration="3"
          |and checkpointLocation="/tmp/cpl3";
        """.stripMargin, ssel)
      Thread.sleep(1000 * 30)
      assume(spark.streams.active.size > 0)
      spark.streams.active.foreach(f => f.stop())
      ScriptSQLExec.parse(
        """
          |load jdbc.`wow_proxy.test1` as output;
        """.stripMargin, ssel)

      val df = spark.sql("select * from output")
      df.show()
      assume(df.count() > 0)
      spark.streams.active.foreach(f => f.stop())

    }
  }

  val kafkaServer = new streaming.test.servers.KafkaServer("1.1.1")
  val mysqlServer = new streaming.test.servers.MySQLServer("5.7")

  override protected def beforeAll(): Unit = {
    kafkaServer.startServer
    kafkaServer.exec("kafka",
      s"""
         |kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic ${topic_name}
      """.stripMargin)
    mysqlServer.startServer
    mysqlServer.exec("mysql", "exec mysql -uroot -pmlsql --protocol=tcp -e 'create database wow'")
  }

  override protected def afterAll(): Unit = {
    kafkaServer.stopServer
    mysqlServer.stopServer
  }
}

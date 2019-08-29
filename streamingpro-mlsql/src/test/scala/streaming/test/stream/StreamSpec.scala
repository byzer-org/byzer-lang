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
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.shell.ShellCommand

class StreamSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {

  val topic_name = "test_cool"

  "kafka8/kafka9" should "kafka work fine on old version" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      // we suppose that if KAFKA_HOME is configured ,then there must be a kafka server exists
      ShellCommand.execCmd("rm -rf /tmp/william/tmp/cpl3")

      new Thread(new Runnable {
        override def run(): Unit = {
          var count = 30
          while (count > 0) {
            val ssel = createSSEL
            try {
              ScriptSQLExec.parse(
                s"""
                   |select "a" as value as tmp_table1;
                   |save append tmp_table1
                   |as kafka8.`${topic_name}` where metadata.broker.list="127.0.0.1:9092";
                  """.stripMargin, ssel)
            } catch {
              case e: Exception => print(e.getMessage)
            }

            Thread.sleep(1000)
            count -= 1
          }

        }
      }).start()

      val ssel = createSSEL
      ScriptSQLExec.parse(
        s"""
           |set streamName="streamExample";
           |load kafka8.`${topic_name}` options kafka.bootstrap.servers="127.0.0.1:9092"
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

  "kafka8/kafka9" should "kafka as sink" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      // we suppose that if KAFKA_HOME is configured ,then there must be a kafka server exists
      ShellCommand.execCmd("rm -rf /tmp/william/tmp/cpl3")

      new Thread(new Runnable {
        override def run(): Unit = {
          var count = 30
          while (count > 0) {
            val ssel = createSSEL
            try {
              ScriptSQLExec.parse(
                s"""
                   |select "a" as value as tmp_table1;
                   |save append tmp_table1
                   |as kafka8.`${topic_name}` where metadata.broker.list="127.0.0.1:9092";
                  """.stripMargin, ssel)
            } catch {
              case e: Exception => print(e.getMessage)
            }

            Thread.sleep(1000)
            count -= 1
          }

        }
      }).start()

      ShellCommand.execCmd("rm -rf /tmp/william/tmp/cpl3")

      val ssel = createSSEL
      ScriptSQLExec.parse(
        s"""
           |set streamName="streamExample";
           |load kafka8.`${topic_name}` options kafka.bootstrap.servers="127.0.0.1:9092"
           |as table1;
           |
           |select cast(value as string) as value from table1 as table2;
           |
           |save append table2
           |as kafka8.`${topic_name}_1`
           |options mode="Append"
           |and duration="2"
           |and kafka.bootstrap.servers="127.0.0.1:9092"
           |and checkpointLocation="/tmp/cpl3";
          """.stripMargin, ssel)
      Thread.sleep(1000 * 30)
      assume(spark.streams.active.size > 0)
      spark.streams.active.foreach(f => f.stop())
    }
  }

  val kafkaServer = new streaming.test.servers.KafkaServer("0.9.0.1")

  override protected def beforeAll(): Unit = {
    kafkaServer.startServer
    kafkaServer.exec("kafka",
      s"""
         |kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic ${topic_name}
      """.stripMargin)
  }

  override protected def afterAll(): Unit = {
    kafkaServer.stopServer
  }
}

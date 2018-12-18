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

import org.apache.spark.SparkCoreVersion
import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.common.shell.ShellCommand
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec

class Stream2Spec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll {
  val KAFKA_1_0_HOME = System.getenv("KAFKA_1_0_HOME")
  val topic_name = "test_cool"
  "kafka" should "work fine on spark 2.3.x" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      // we suppose that if KAFKA_HOME is configured ,then there must be a kafka server exists
      if (KAFKA_1_0_HOME != null) {

        ShellCommand.execCmd("rm -rf /tmp/cpl3")

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
        assume(spark.streams.active.size>0)
        spark.streams.active.foreach(f => f.stop())
      }

    }
  }

  val kafkaServer = new KafkaServer(Option(KAFKA_1_0_HOME), Seq(topic_name))

  override protected def beforeAll(): Unit = {
    if (KAFKA_1_0_HOME != null) {
      kafkaServer.startServer
    }
  }

  override protected def afterAll(): Unit = {
    if (KAFKA_1_0_HOME != null) {
      kafkaServer.shutdownServer
    }

  }
}

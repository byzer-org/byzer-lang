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

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.shell.ShellCommand

/**
  * 2018-12-11 WilliamZhu(allwefantasy@gmail.com)
  */
class KafkaServer(_home: Option[String], topicNames: Seq[String]) extends Logging {
  def kafkaHome = {
    _home match {
      case Some(item) => item
      case None =>
        require(System.getenv("KAFKA_HOME") != null)
        System.getenv("KAFKA_HOME")
    }
  }

  def kafkaIsReady() = {
    val shellCommand = s"cd $kafkaHome;./bin/kafka-topics.sh --list  --zookeeper localhost:2181"
    readyCheck(shellCommand)
  }

  def zkIsReady(): Boolean = {
    val shellCommand = s"echo stat | nc 127.0.0.1 2181"
    readyCheck(shellCommand)
  }

  private def readyCheck(command: String): Boolean = {
    //echo stat | nc 127.0.0.1 2182
    var counter = 60
    var success = false
    while (!success && counter > 0) {
      val (status, out, error) = ShellCommand.execWithExitValue(command, -1)
      if (status == 0) {
        success = true
      }
      Thread.sleep(1000)
      logInfo(s"execute command:${command}")
      logInfo(s"checking count: ${counter} status:${status} out:${out} error:${error}")
      counter -= 1
    }
    success
  }

  def startServer = {
    ShellCommand.exec(s"cd $kafkaHome;./bin/zookeeper-server-start.sh -daemon config/zookeeper.properties")
    if (zkIsReady()) {
      ShellCommand.exec(s"cd $kafkaHome;./bin/kafka-server-start.sh -daemon config/server.properties")
      if (kafkaIsReady) {
        topicNames.foreach { topicName =>
          val (status, out, error) = ShellCommand.execWithExitValue(s"cd $kafkaHome;./bin/kafka-topics.sh --create --if-not-exists --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ${topicName}", -1)
          // kafka version lower 1.0 have no option `if-not-exists`, we check it 
          if ((out + error).contains("""'if-not-exists' is not a recognized option""")) {
            val (status, out, error) = ShellCommand.execWithExitValue(s"cd $kafkaHome;./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ${topicName}", -1)
            if ((out + error).contains("kafka.common.TopicExistsException")) {
              logInfo(s"$topicName is aready exists")
            } else {
              require(status == 0, "fail to create topic")
            }
          }

        }
      } else {
        new RuntimeException("fail to start kafka")
      }
    } else {
      throw new RuntimeException("fail to start zk")
    }
  }

  private def checkProcessNotExistsByName(name: String) = {
    val (status, out, error) = ShellCommand.execWithExitValue("$(ps ax | grep java | grep -i " + name + " | grep -v grep | awk '{print $1}')")
    (out + error).isEmpty
  }

  private def waitProcessNotExistsByName(name: String) = {
    //echo stat | nc 127.0.0.1 2182
    var counter = 60
    var success = false
    while (!success && counter > 0) {
      if (checkProcessNotExistsByName(name)) {
        success = true
      }
      Thread.sleep(1000)
      counter -= 1
    }
    success
  }

  private def _shutdownZk = {
    var res = ""
    try {
      res = ShellCommand.exec(s"cd $kafkaHome;./bin/zookeeper-server-stop.sh")
    } catch {
      case e: Exception => {
        logError(s"execute command: [cd $kafkaHome;./bin/zookeeper-server-stop.sh] fail ->${res} ", e)
      }
    }

  }

  private def _shutdownKafka = {
    var res = ""
    try {
      res = ShellCommand.exec(s"cd $kafkaHome;./bin/kafka-server-stop.sh")
    } catch {
      case e: Exception =>
        logError(s"execute command: [cd $kafkaHome;./bin/kafka-server-stop.sh] fail -> ${res}", e)
    }
  }

  def shutdownServer = {
    _shutdownKafka
    if (waitProcessNotExistsByName("'kafka\\.Kafka'")) {
      logInfo("shutdown kafka server")
      _shutdownZk
      if (waitProcessNotExistsByName("QuorumPeerMain")) {
        logInfo("shutdown zk server")
      } else {
        throw new RuntimeException("fail to shutdown zk")
      }
    } else {
      throw new RuntimeException("fail to shutdown kafka")
    }

  }
}

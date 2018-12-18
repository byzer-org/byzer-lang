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

package com.hortonworks.spark.sql.kafka08

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by allwefantasy on 23/4/2018.
  */
object KafkaOperator {

  def filterScore(str: String) = {
    if (str != null && str.startsWith("mlsql_validation_score:")) {
      str.split(":").last.toDouble
    } else 0d
  }

  def writeKafka(prefix: String, kafkaParam: Map[String, String], lines: Iterator[String], logCallback: (String) => Unit = (msg: String) => {}) = {

    if (!kafkaParam.contains("userName")) {
      lines.map {
        f =>
          logCallback(prefix + "" + f)
          filterScore(f)
      }.filter(f => f > 0d).toSeq
    } else {
      val topic = "training_msg_" + kafkaParam("userName")

      val props = new Properties()
      kafkaParam.foreach(f => props.put(f._1, f._2))


      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
      try {
        lines.map { line =>
          logCallback(prefix + "" + line)
          producer.send(new ProducerRecord[String, String](topic, prefix + "" + line))
          filterScore(line)
        }.filter(f => f > 0d).toSeq
      } finally {
        producer.close()
      }
    }
  }

}

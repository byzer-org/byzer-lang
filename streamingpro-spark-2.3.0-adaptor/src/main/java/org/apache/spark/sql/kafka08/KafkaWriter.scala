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

package org.apache.spark.sql.kafka08

import java.{util => ju}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.sql.{AnalysisException, SparkSession}

/**
  * Created by allwefantasy on 3/7/2017.
  */
private[kafka08] object KafkaWriter extends Logging {

  val TOPIC_ATTRIBUTE_NAME: String = "topic"
  val KEY_ATTRIBUTE_NAME: String = "key"
  val VALUE_ATTRIBUTE_NAME: String = "value"

  override def toString: String = "KafkaWriter"

  def validateQuery(
                     queryExecution: QueryExecution,
                     kafkaParameters: ju.Map[String, Object],
                     topic: Option[String] = None): Unit = {
    val schema = queryExecution.logical.output
    schema.find(_.name == TOPIC_ATTRIBUTE_NAME).getOrElse(
      if (topic == None) {
        throw new AnalysisException(s"topic option required when no " +
          s"'$TOPIC_ATTRIBUTE_NAME' attribute is present.")
      } else {
        Literal(topic.get, StringType)
      }
    ).dataType match {
      case StringType => // good
      case _ =>
        throw new AnalysisException(s"Topic type must be a String")
    }
    schema.find(_.name == KEY_ATTRIBUTE_NAME).getOrElse(
      Literal(null, StringType)
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(s"$KEY_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
    schema.find(_.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new AnalysisException(s"Required attribute '$VALUE_ATTRIBUTE_NAME' not found")
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(s"$VALUE_ATTRIBUTE_NAME attribute type " +
          s"must be a String or BinaryType")
    }
  }

  def write(
             sparkSession: SparkSession,
             queryExecution: QueryExecution,
             kafkaParameters: ju.Map[String, Object],
             topic: Option[String] = None): Unit = {
    val schema = queryExecution.logical.output
    validateQuery(queryExecution, kafkaParameters, topic)
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      queryExecution.toRdd.foreachPartition { iter =>
        val writeTask = new KafkaWriteTask(kafkaParameters, schema, topic)
        org.apache.spark.util.Utils.tryWithSafeFinally(block = writeTask.execute(iter))(
          finallyBlock = writeTask.close())
      }
    }
  }
}

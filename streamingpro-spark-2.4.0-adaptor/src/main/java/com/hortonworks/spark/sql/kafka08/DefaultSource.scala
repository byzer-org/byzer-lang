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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.kafka08

import java.{util => ju}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import com.hortonworks.spark.sql.kafka08.util.Logging
import org.apache.spark.sql.kafka08.{KafkaSink, KafkaWriteTask}
import org.apache.spark.sql.streaming.OutputMode

import scala.tools.scalap.scalax.rules.scalasig.Attribute

/**
  * The provider class for the [[KafkaSource]]. This provider is designed such that it throws
  * IllegalArgumentException when the Kafka Dataset is created, so that it can catch
  * missing options even before the query is started.
  */
private[kafka08] class DefaultSource extends StreamSourceProvider with CreatableRelationProvider
  with DataSourceRegister with StreamSinkProvider with Logging {

  import DefaultSource._

  /**
    * Returns the name and schema of the source. In addition, it also verifies whether the options
    * are correct and sufficient to create the [[KafkaSource]] when the query is started.
    */
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "Kafka source has a fixed schema and cannot be set with a custom one")
    validateOptions(parameters)
    ("kafka", KafkaSource.kafkaSchema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {
    validateOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }
    val specifiedKafkaParams =
      parameters
        .keySet
        .filter(_.toLowerCase.startsWith("kafka."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap

    val topics =
      caseInsensitiveParams.get(TOPICS) match {
        case Some(s) => s.split(",").map(_.trim).filter(_.nonEmpty).toSet
        case None => throw new IllegalArgumentException(s"$TOPICS should be set.")
      }

    val startFromEarliestOffset =
      caseInsensitiveParams.get(STARTING_OFFSET_OPTION_KEY).map(_.trim.toLowerCase) match {
        case Some("largest") => false
        case Some("smallest") => true
        case Some(pos) =>
          // This should not happen since we have already checked the options.
          throw new IllegalStateException(s"Invalid $STARTING_OFFSET_OPTION_KEY: $pos")
        case None => false
      }

    val kafkaParams =
      ConfigUpdater("source", specifiedKafkaParams)
        // Set to "largest" to avoid exceptions. However, KafkaSource will fetch the initial offsets
        // by itself instead of counting on KafkaConsumer.
        .set(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest")

        // So that consumers does not commit offsets unnecessarily
        .set("auto.commit.enable", "false")
        //.setIfUnset(ConsumerConfig.SOCKET_RECEIVE_BUFFER_CONFIG, "65536")
        .set(ConsumerConfig.GROUP_ID_CONFIG, "")
        .set("zookeeper.connect", "")
        .build()

    new KafkaSource(
      sqlContext,
      topics,
      kafkaParams.asScala.toMap,
      parameters,
      metadataPath,
      startFromEarliestOffset)
  }

  private def validateOptions(parameters: Map[String, String]): Unit = {

    // Validate source options
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }

    caseInsensitiveParams.get(STARTING_OFFSET_OPTION_KEY) match {
      case Some(pos) if !STARTING_OFFSET_OPTION_VALUES.contains(pos.trim.toLowerCase) =>
        throw new IllegalArgumentException(
          s"Illegal value '$pos' for option '$STARTING_OFFSET_OPTION_KEY', " +
            s"acceptable values are: ${STARTING_OFFSET_OPTION_VALUES.mkString(", ")}")
      case _ =>
    }

    // Validate user-specified Kafka options
    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.GROUP_ID_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.GROUP_ID_CONFIG}' is not supported as " +
          s"user-specified consumer groups is not used to track offsets.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}")) {
      throw new IllegalArgumentException(
        s"""
           |Kafka option '${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}' is not supported.
           |Instead set the source option '$STARTING_OFFSET_OPTION_KEY' to 'largest' or
           |'smallest' to specify where to start. Structured Streaming manages which offsets are
           |consumed internally, rather than relying on the kafkaConsumer to do it. This will
           |ensure that no data is missed when when new topics/partitions are dynamically
           |subscribed. Note that '$STARTING_OFFSET_OPTION_KEY' only applies when a new Streaming
           |query is started, and that resuming will always pick up from where the query left
           |off. See the docs for more
           |details.
         """.stripMargin)
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}' is not supported as keys "
          + "are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame operations "
          + "to explicitly deserialize the keys.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}' is not supported as "
          + "value are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame "
          + "operations to explicitly deserialize the values.")
    }

    val otherUnsupportedConfigs = Seq(
      "auto.commit.enable", // committing correctly requires new APIs in Source
      "zookeeper.connect")

    otherUnsupportedConfigs.foreach { c =>
      if (caseInsensitiveParams.contains(s"kafka.$c")) {
        throw new IllegalArgumentException(s"Kafka option '$c' is not supported")
      }
    }

    if (!caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}") &&
      !caseInsensitiveParams.contains(s"kafka.metadata.brokers.list")) {
      throw new IllegalArgumentException(
        s"Option 'kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}' or " +
          "'kafka.metadata.broker.list' must be specified for configuring Kafka consumer")
    }
  }

  override def shortName(): String = "kafka"

  /** Class to conveniently update Kafka config params, while logging the changes */
  private case class ConfigUpdater(module: String, kafkaParams: Map[String, String]) {
    private val map = new ju.HashMap[String, String](kafkaParams.asJava)

    def set(key: String, value: String): this.type = {
      map.put(key, value)
      info(s"$module: Set $key to $value, earlier value: ${kafkaParams.get(key).getOrElse("")}")
      this
    }

    def setIfUnset(key: String, value: String): ConfigUpdater = {
      if (!map.containsKey(key)) {
        map.put(key, value)
        info(s"$module: Set $key to $value")
      }
      this
    }

    def build(): ju.Map[String, String] = map
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val defaultTopic = parameters.get(TOPICS).map(_.trim)
    new KafkaSink(sqlContext,
      new ju.HashMap[String, Object](parameters.asJava), defaultTopic)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation = InsertKafkaRelation(data, parameters)(sqlContext)
    relation.insert(data, SaveMode.Overwrite == mode)
    relation
  }
}

case class InsertKafkaRelation(
                                dataFrame: DataFrame,
                                parameters: Map[String, String]
                              )(@transient val sqlContext: SQLContext)
  extends BaseRelation with InsertableRelation with Logging {

  override def schema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType)))


  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    import scala.collection.JavaConversions._
    val qe = dataFrame.toJSON.queryExecution
    val inputSchema = qe.logical.output
    qe.toRdd.foreachPartition { iter =>
      val writeTask = new KafkaWriteTask(parameters - "topic", inputSchema, parameters.get("topics"))
      tryWithSafeFinally(block = writeTask.execute(iter))(
        finallyBlock = writeTask.close())
    }
  }

  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            logger.warn(s"Suppressing exception in finally: " + t.getMessage, t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }
}

private[kafka08] object DefaultSource {
  private val TOPICS = "topics"
  private val STARTING_OFFSET_OPTION_KEY = "startingoffset"
  private val STARTING_OFFSET_OPTION_VALUES = Set("largest", "smallest")
}


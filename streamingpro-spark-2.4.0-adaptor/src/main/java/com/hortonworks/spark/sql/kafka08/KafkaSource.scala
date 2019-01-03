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

import com.hortonworks.spark.sql.kafka08.KafkaSource._
import com.hortonworks.spark.sql.kafka08.util.Logging
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{Broker, KafkaCluster, KafkaUtils, OffsetRange}

import scala.annotation.tailrec

/**
  * A [[Source]] that uses Kafka's SimpleConsumer API to reads data from Kafka.
  */
case class KafkaSource(
                        sqlContext: SQLContext,
                        topics: Set[String],
                        kafkaParams: Map[String, String],
                        sourceOptions: Map[String, String],
                        metadataPath: String,
                        startFromSmallestOffset: Boolean)
  extends Source with Logging {

  private val sc = sqlContext.sparkContext
  private val kc = new KafkaCluster(kafkaParams)
  private val topicPartitions = KafkaCluster.checkErrors(kc.getPartitions(topics))

  private val maxOffsetFetchAttempts =
    sourceOptions.getOrElse("fetchOffset.numRetries", "3").toInt

  private lazy val initialPartitionOffsets = {
    val metadataLog = Kafka08HDFSMetadataLog.create(sqlContext, metadataPath, VERSION)
    metadataLog.get(0).getOrElse {
      val offsets = for {
        leaderOffsets <- (if (startFromSmallestOffset) {
          kc.getEarliestLeaderOffsets(topicPartitions)
        } else {
          kc.getLatestLeaderOffsets(topicPartitions)
        }).right
      } yield leaderOffsets

      val kafkaSourceOffset = KafkaSourceOffset(KafkaCluster.checkErrors(offsets))

      metadataLog.add(0, kafkaSourceOffset)
      info(s"Initial offsets: $kafkaSourceOffset")
      kafkaSourceOffset
    }.partitionToOffsets
  }

  override def schema: StructType = KafkaSource.kafkaSchema

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    val offset = KafkaSourceOffset(fetchLatestOffsets(maxOffsetFetchAttempts))
    debug(s"GetOffset: ${offset.partitionToOffsets.toSeq.map(_.toString).sorted}")
    Some(offset)
  }

  /**
    * Returns the data that is between the offsets
    * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
    * exclusive.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    info(s"GetBatch called with start = $start, end = $end")
    val untilPartitionOffsets = KafkaSourceOffset.getPartitionOffsets(end)
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        KafkaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    val offsetRanges = fromPartitionOffsets.map { case (tp, fo) =>
      val uo = untilPartitionOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo.offset, uo.offset)
    }.toArray

    val leaders = untilPartitionOffsets.map { case (tp, lo) =>
      tp -> Broker(lo.host, lo.port)
    }

    val messageHandler = (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => {
      Row(mmd.key(), mmd.message(), mmd.topic, mmd.partition, mmd.offset)
    }

    val enconder = RowEncoder.apply(schema).resolveAndBind()
    // Create a RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = KafkaUtils.createRDD[
      Array[Byte],
      Array[Byte],
      DefaultDecoder,
      DefaultDecoder,
      Row](sc, kafkaParams, offsetRanges, leaders, messageHandler).map(row => enconder.toRow(row))

    info("GetBatch generating RDD of offset range: " + offsetRanges.sortBy(_.topic).mkString(","))
    new SessionWrapper(sqlContext.sparkSession).internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {}

  override def toString(): String = s"KafkaSource for topics [${topics.mkString(",")}]"

  /**
    * Fetch the latest offset of partitions.
    */
  @tailrec
  private def fetchLatestOffsets(retries: Int): Map[TopicAndPartition, LeaderOffset] = {
    val offsets = kc.getLatestLeaderOffsets(topicPartitions)
    if (offsets.isLeft) {
      val err = offsets.left.get.toString
      if (retries <= 0) {
        throw new SparkException(err)
      } else {
        error(err)
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        fetchLatestOffsets(retries - 1)
      }
    } else {
      offsets.right.get
    }
  }
}

/** Companion object for the [[KafkaSource]]. */
private[kafka08] object KafkaSource {
  private[kafka08] val VERSION = 8

  def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType)
  ))
}


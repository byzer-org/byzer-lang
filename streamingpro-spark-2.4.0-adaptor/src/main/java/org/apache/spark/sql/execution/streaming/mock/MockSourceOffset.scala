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

package org.apache.spark.sql.execution.streaming.mock

import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}


/**
  * Created by allwefantasy on 20/8/2018.
  */

case class MockSourceOffset(partitionToOffsets: Map[TopicPartition, Long]) extends Offset {

  override val json = JsonUtils.partitionOffsets(partitionToOffsets)
}

object MockSourceOffset {
  def getPartitionOffsets(offset: Offset): Map[TopicPartition, Long] = {
    offset match {
      case o: MockSourceOffset => o.partitionToOffsets
      case so: SerializedOffset => MockSourceOffset(so).partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }

  /**
    * Returns [[MockSourceOffset]] from a variable sequence of (topic, partitionId, offset)
    * tuples.
    */
  def apply(offsetTuples: (String, Int, Long)*): MockSourceOffset = {
    MockSourceOffset(offsetTuples.map { case (t, p, o) => (new TopicPartition(t, p), o) }.toMap)
  }

  /**
    * Returns [[MockSourceOffset]] from a JSON [[SerializedOffset]]
    */
  def apply(offset: SerializedOffset): MockSourceOffset =
  MockSourceOffset(JsonUtils.partitionOffsets(offset.json))
}

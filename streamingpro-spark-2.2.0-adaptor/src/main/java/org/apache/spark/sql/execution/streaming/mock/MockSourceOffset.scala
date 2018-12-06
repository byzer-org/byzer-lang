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

package org.apache.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.StreamInputInfo

import scala.reflect.ClassTag

/**
 * 2/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
class TestInputStream[T: ClassTag](_ssc: StreamingContext, input: Seq[Seq[T]], numPartitions: Int)
  extends InputDStream[T](_ssc) {

  def start() {}

  def stop() {}

  var currentOffset: Int = 0

  def compute(validTime: Time): Option[RDD[T]] = {
    logInfo("Computing RDD for time " + validTime)

    val selectedInput = if (currentOffset < input.size) input(currentOffset) else input(0)

    // lets us test cases where RDDs are not created
    if (selectedInput == null) {
      currentOffset = 0
    }
    // Report the input data's information to InputInfoTracker for testing

    val metadata = Map(
      "offsets" -> currentOffset,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> "")

    val inputInfo = StreamInputInfo(id, selectedInput.length.toLong, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    currentOffset = currentOffset + 1
    val rdd = ssc.sc.makeRDD(selectedInput, numPartitions)
    logInfo("Created RDD " + rdd.id + " with index" + currentOffset)
    Some(rdd)
  }
}

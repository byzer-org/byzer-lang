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

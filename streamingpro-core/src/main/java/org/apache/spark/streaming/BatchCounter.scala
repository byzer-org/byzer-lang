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

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted}

/**
 * 4/26/16 WilliamZhu(allwefantasy@gmail.com)
 */
class BatchCounter(ssc: StreamingContext) {

  // All access to this state should be guarded by `BatchCounter.this.synchronized`
  private var numCompletedBatches = 0
  private var numStartedBatches = 0
  private var lastCompletedBatchTime: Time = null

  private val listener = new StreamingListener {
    override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit =
      BatchCounter.this.synchronized {
        numStartedBatches += 1
        BatchCounter.this.notifyAll()
      }
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =
      BatchCounter.this.synchronized {
        numCompletedBatches += 1
        lastCompletedBatchTime = batchCompleted.batchInfo.batchTime
        BatchCounter.this.notifyAll()
      }
  }
  ssc.addStreamingListener(listener)

  def getNumCompletedBatches: Int = this.synchronized {
    numCompletedBatches
  }

  def getNumStartedBatches: Int = this.synchronized {
    numStartedBatches
  }

  def getLastCompletedBatchTime: Time = this.synchronized {
    lastCompletedBatchTime
  }

  /**
   * Wait until `expectedNumCompletedBatches` batches are completed, or timeout. Return true if
   * `expectedNumCompletedBatches` batches are completed. Otherwise, return false to indicate it's
   * timeout.
   *
   * @param expectedNumCompletedBatches the `expectedNumCompletedBatches` batches to wait
   * @param timeout the maximum time to wait in milliseconds.
   */
  def waitUntilBatchesCompleted(expectedNumCompletedBatches: Int, timeout: Long): Boolean =
    waitUntilConditionBecomeTrue(numCompletedBatches >= expectedNumCompletedBatches, timeout)

  /**
   * Wait until `expectedNumStartedBatches` batches are completed, or timeout. Return true if
   * `expectedNumStartedBatches` batches are completed. Otherwise, return false to indicate it's
   * timeout.
   *
   * @param expectedNumStartedBatches the `expectedNumStartedBatches` batches to wait
   * @param timeout the maximum time to wait in milliseconds.
   */
  def waitUntilBatchesStarted(expectedNumStartedBatches: Int, timeout: Long): Boolean =
    waitUntilConditionBecomeTrue(numStartedBatches >= expectedNumStartedBatches, timeout)

  private def waitUntilConditionBecomeTrue(condition: => Boolean, timeout: Long): Boolean = {
    synchronized {
      var now = System.currentTimeMillis()
      val timeoutTick = now + timeout
      while (!condition && timeoutTick > now) {
        wait(timeoutTick - now)
        now = System.currentTimeMillis()
      }
      condition
    }
  }
}

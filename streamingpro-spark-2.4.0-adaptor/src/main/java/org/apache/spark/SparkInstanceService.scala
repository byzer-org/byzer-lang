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

package org.apache.spark

import java.util.concurrent.atomic.AtomicInteger

import net.csdn.common.reflect.ReflectHelper
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, StandaloneSchedulerBackend}
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.sql.SparkSession

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
class SparkInstanceService(session: SparkSession) {

  def resources = {
    var totalTasks = 0l
    var totalUsedMemory = 0l
    var totalMemory = 0l
    session.sparkContext.statusTracker.getExecutorInfos.map { worker =>
      totalTasks += worker.numRunningTasks()
      totalUsedMemory += (worker.usedOnHeapStorageMemory() + worker.usedOffHeapStorageMemory())
      totalMemory += (worker.totalOnHeapStorageMemory() + worker.totalOffHeapStorageMemory())

    }
    val totalCores = session.sparkContext.schedulerBackend match {
      case sb if sb.isInstanceOf[CoarseGrainedSchedulerBackend] =>
        ReflectHelper.field(sb, "totalCoreCount").asInstanceOf[AtomicInteger].get()
      case sb if sb.isInstanceOf[LocalSchedulerBackend] =>
        //val k8sDetect = System.getenv().get("KUBERNETES_SERVICE_HOST")
        java.lang.Runtime.getRuntime.availableProcessors
      case sb if sb.isInstanceOf[StandaloneSchedulerBackend] => -1
    }
    SparkInstanceResource(totalCores.toLong, totalTasks, totalUsedMemory, totalMemory)
  }
  
}

case class SparkInstanceResource(totalCores: Long, totalTasks: Long, totalUsedMemory: Long, totalMemory: Long)

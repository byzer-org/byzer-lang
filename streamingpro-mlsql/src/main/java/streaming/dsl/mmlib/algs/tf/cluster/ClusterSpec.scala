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

package streaming.dsl.mmlib.algs.tf.cluster

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}
import scala.collection.mutable

/**
  * Created by allwefantasy on 16/7/2018.
  */
class ClusterSpec(val worker: List[String], val ps: List[String]) {

  def indexer = {
    worker.zipWithIndex.map(f => f._2)
  }

  def workerTasks = {
    worker.zipWithIndex.map(f => s"/job:worker/task:${f._2}")
  }

  def psTasks = {
    ps.zipWithIndex.map(f => s"/job:ps/task:${f._2}")
  }
}

case class ExecutorInfo(host: String, port: Int, jobName: String, taskIndex: Int)


object ClusterStatus {

  val loader = new CacheLoader[String, mutable.HashSet[ExecutorInfo]]() {
    override def load(key: String): mutable.HashSet[ExecutorInfo] = {
      new mutable.HashSet[ExecutorInfo]()
    }
  }
  val tfworkers = CacheBuilder.newBuilder().
    maximumSize(100).
    expireAfterAccess(30, TimeUnit.MINUTES).
    build[String, mutable.HashSet[ExecutorInfo]](loader)

  def count(cluster: String) = {
    tfworkers.get(cluster).size
  }

  def count(cluster: String, executorInfo: ExecutorInfo) = {
    synchronized {
      val infos = tfworkers.get(cluster)
      infos.add(executorInfo)
    }
  }
}

object ClusterSpec {
  val MIN_PORT_NUMBER = 2221
  val MAX_PORT_NUMBER = 6666

  val loader = new CacheLoader[String, mutable.HashSet[ExecutorInfo]]() {
    override def load(key: String): mutable.HashSet[ExecutorInfo] = {
      new mutable.HashSet[ExecutorInfo]()
    }
  }
  val tfJobs = CacheBuilder.newBuilder().
    maximumSize(10).
    expireAfterAccess(5, TimeUnit.MINUTES).
    build[String, mutable.HashSet[ExecutorInfo]](loader)


  def clusterSpec(cluster: String) = {
    tfJobs.get(cluster)
  }

  def addJob(cluster: String, executorInfo: ExecutorInfo) = {
    synchronized {
      val infos = tfJobs.get(cluster)
      infos.add(executorInfo)
    }
  }
}

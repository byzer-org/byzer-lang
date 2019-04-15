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

import java.net.Socket
import java.util.Properties

import org.apache.spark.api.python.WowPythonWorkerFactory

import scala.collection.mutable

/**
  * Created by allwefantasy on 30/7/2018.
  */
object APIDeployPythonRunnerEnv {
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), WowPythonWorkerFactory]()

  def workerSize = {
    pythonWorkers.size
  }

  def generate_key(daemonCommand: Option[Seq[String]],
                   workerCommand: Option[Seq[String]]) = {
    daemonCommand.get.mkString(" ") + workerCommand.get.mkString(" ")
  }

  def createPythonWorker(daemonCommand: Option[Seq[String]],
                         workerCommand: Option[Seq[String]],
                         envVars: Map[String, String],
                         logCallback: (String) => Unit,
                         idleWorkerTimeoutMS: Long,
                         noCache: Boolean = true
                        ): java.net.Socket = {
    synchronized {
      val key = (generate_key(daemonCommand, workerCommand), envVars)
      if (noCache) {
        pythonWorkers.getOrElseUpdate(key, new WowPythonWorkerFactory(
          daemonCommand,
          workerCommand,
          envVars,
          logCallback,
          idleWorkerTimeoutMS)).create()
      } else {
        new WowPythonWorkerFactory(
          daemonCommand,
          workerCommand,
          envVars,
          logCallback,
          idleWorkerTimeoutMS).create()
      }


    }
  }


  def destroyPythonWorker(daemonCommand: Option[Seq[String]],
                          workerCommand: Option[Seq[String]],
                          envVars: Map[String, String],
                          worker: Socket) {
    synchronized {
      val key = (generate_key(daemonCommand, workerCommand), envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }


  def releasePythonWorker(daemonCommand: Option[Seq[String]],
                          workerCommand: Option[Seq[String]],
                          envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (generate_key(daemonCommand, workerCommand), envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }

  def setTaskContext(context: TaskContext) = {
    TaskContext.setTaskContext(context)
  }

  def createTaskContext() = {
    new TaskContextImpl(0, 0, 0, -1024, 0, null, new Properties, null)
  }
}

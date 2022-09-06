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


/**
  * Created by allwefantasy on 30/7/2018.
  */
class MLSQLPythonEnv(env: SparkEnv, deployAPI: Boolean) {

  def sparkEnv = env

  def createPythonWorker(daemonCommand: Option[Seq[String]],
                         workerCommand: Option[Seq[String]],
                         envVars: Map[String, String],
                         logCallback: (String) => Unit,
                         idleWorkerTimeoutMS: Long,
                         noCache: Boolean = true
                        ): java.net.Socket = {
    APIDeployPythonRunnerEnv.createPythonWorker(daemonCommand, workerCommand, envVars, logCallback, idleWorkerTimeoutMS, noCache)
  }


  def destroyPythonWorker(daemonCommand: Option[Seq[String]],
                          workerCommand: Option[Seq[String]],
                          envVars: Map[String, String], worker: Socket) {
    APIDeployPythonRunnerEnv.destroyPythonWorker(daemonCommand, workerCommand, envVars, worker)
  }


  def releasePythonWorker(daemonCommand: Option[Seq[String]],
                          workerCommand: Option[Seq[String]],
                          envVars: Map[String, String], worker: Socket) {
    APIDeployPythonRunnerEnv.releasePythonWorker(daemonCommand, workerCommand, envVars, worker)
  }
}

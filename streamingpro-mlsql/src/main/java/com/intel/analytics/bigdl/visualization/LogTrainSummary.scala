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

package com.intel.analytics.bigdl.visualization

import streaming.log.{Logging, WowLog}

class LogTrainSummary(logDir: String,
                      appName: String) extends TrainSummary(logDir, appName) with Logging with WowLog {

  override def addScalar(tag: String, value: Float, step: Long): LogTrainSummary.this.type = {
    //    tag match {
    //      case "Throughput" =>
    //        logInfo(format(s"global step: ${step}  Throughput is ${value} records/second. "))
    //      case "Loss" =>
    //        logInfo(format(s"global step: ${step}  Loss is ${value}"))
    //      case _ =>
    //        logInfo(format(s"global step: ${step}  ${tag} is ${value}"))
    //    }

    super.addScalar(tag, value, step)
  }
}

class LogValidateSummary(logDir: String,
                         appName: String) extends ValidationSummary(logDir, appName) with Logging with WowLog {
  override def addScalar(tag: String, value: Float, step: Long): LogValidateSummary.this.type = {
    //logInfo(format(s"global step: ${step}  ${tag} is ${value}"))
    super.addScalar(tag, value, step)
  }
}

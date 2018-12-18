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

package streaming.dsl.mmlib.algs

import java.util.concurrent.ConcurrentHashMap

import com.salesforce.op.WowOpWorkflow
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 20/9/2018.
  */
object WorkflowManager {
  private val workflows = new ConcurrentHashMap[String, OpWorkflowInfo]()

  def get(name: String) = {
    workflows.get(name)
  }

  def put(name: String, opWorkflowInfo: OpWorkflowInfo) = {
    workflows.put(name, opWorkflowInfo)
  }

  def items = {
    workflows.values().toSeq
  }
}

case class OpWorkflowInfo(name: String, wowOpWorkflow: WowOpWorkflow)

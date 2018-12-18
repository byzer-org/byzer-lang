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

package streaming.remote

import net.csdn.annotation.Param
import net.csdn.annotation.rest.At
import net.csdn.modules.http.RestRequest
import net.csdn.modules.transport.HttpTransportService
import net.liftweb.{json => SJSon}

/**
  * 3/10/16 WilliamZhu(allwefantasy@gmail.com)
  */
trait YarnController {

  @At(path = Array("/ws/v1/cluster/apps"), types = Array(RestRequest.Method.GET))
  def apps(@Param("states") states: String): java.util.List[HttpTransportService.SResponse]

  @At(path = Array("/ws/v1/cluster/apps/{appId}"), types = Array(RestRequest.Method.GET))
  def app(@Param("appId") appId: String): java.util.List[HttpTransportService.SResponse]

}

object YarnControllerE {
  implicit def mapSResponseToObject(response: java.util.List[HttpTransportService.SResponse]): SResponseEnhance = {
    new SResponseEnhance(response)
  }
}

import scala.collection.JavaConversions._

class SResponseEnhance(x: java.util.List[HttpTransportService.SResponse]) {

  private def extract[T](res: String)(implicit manifest: Manifest[T]): T = {
    if (x == null || x.isEmpty || x(0).getStatus != 200) {
      return null.asInstanceOf[T]
    }
    implicit val formats = SJSon.DefaultFormats
    SJSon.parse(res).extract[T]
  }

  private def validate = {
    if (x == null || x.isEmpty || x(0).getStatus != 200) {
      false
    }
    else true
  }

  def apps(): List[YarnApplication] = {
    if (validate) return List()
    val item = extract[Map[String, Map[String, List[YarnApplication]]]](x(0).getContent)
    return item("apps")("app")
  }

  def app(): List[YarnApplication] = {
    if (!validate) return List()
    val item = extract[Map[String, YarnApplication]](x(0).getContent)
    return List(item("app"))
  }
}

object YarnApplicationState extends Enumeration {
  type YarnApplicationState = Value
  val NEW = Value("NEW")
  val NEW_SAVING = Value("NEW_SAVING")
  val SUBMITTED = Value("SUBMITTED")
  val ACCEPTED = Value("ACCEPTED")
  val RUNNING = Value("RUNNING")
  val FINISHED = Value("FINISHED")
  val FAILED = Value("FAILED")
  val KILLED = Value("KILLED")
}


class YarnApplication(val id: String,
                      var user: String,
                      var name: String,
                      var queue: String,
                      var state: String,
                      var finalStatus: String,
                      var progress: Long,
                      var trackingUI: String,
                      var trackingUrl: String,
                      var diagnostics: String,
                      var clusterId: Long,
                      var applicationType: String,
                      var applicationTags: String,
                      var startedTime: Long,
                      var finishedTime: Long,
                      var elapsedTime: Long,
                      var amContainerLogs: String,
                      var amHostHttpAddress: String,
                      var allocatedMB: Long,
                      var allocatedVCores: Long,
                      var runningContainers: Long,
                      var memorySeconds: Long,
                      var vcoreSeconds: Long,
                      var preemptedResourceMB: Long,
                      var preemptedResourceVCores: Long,
                      var numNonAMContainerPreempted: Long,
                      var numAMContainerPreempted: Long)

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

package tech.mlsql.cluster.controller

import net.csdn.annotation.rest._
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method.{GET, POST}
import tech.mlsql.cluster.model.{EcsResourcePool, ElasticMonitor}

import scala.collection.JavaConverters._

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
@OpenAPIDefinition(
  info = new BasicInfo(
    desc = "The collection of rest api are used to manager allocate backends.",
    state = State.alpha,
    contact = new Contact(url = "https://github.com/allwefantasy", name = "WilliamZhu", email = "allwefantasy@gmail.com"),
    license = new License(name = "Apache-2.0", url = "https://github.com/allwefantasy/streamingpro/blob/master/LICENSE")),
  externalDocs = new ExternalDocumentation(description =
    """

    """),
  servers = Array()
)
class EcsResourceController extends ApplicationController {
  @Action(
    summary = "add allocate template", description = "mlsql cluster will use this template to start mlsql instance. notice that once the new instance is started ,this row will be updated to in_use status."
  )
  @Parameters(Array(
    new Parameter(name = "ip", required = true, description = "the server where we can start new mlsql instance", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "keyPath", required = true, description = "mlsql-cluster use the keyPath to ssh server wihout password", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "name", required = true, description = "the server name", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "loginUser", required = true, description = "the username used to login server", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "sparkHome", required = true, description = "spark home", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "mlsqlHome", required = true, description = "mlsql home", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "mlsqlConfig", required = true, description = "configuration of starting mlsql instance,json foramt", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "executeUser", required = true, description = "the user runs mlsql instance", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "tag", required = true, description = "tag the server", `type` = "string", allowEmptyValue = false)

  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/ecs/add"), types = Array(GET, POST))
  def ecsAdd = {
    EcsResourcePool.requiredFields().asScala.foreach(item => require(hasParam(item), s"${item} is required"))
    EcsResourcePool.newOne(params(), true)
    render(map("msg", "success"))
  }

  @Action(
    summary = "remove templte", description = ""
  )
  @Parameters(Array(
    new Parameter(name = "id", required = true, description = "the id of ecs", `type` = "string", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/ecs/remove"), types = Array(GET, POST))
  def ecsRemove = {
    val backend = EcsResourcePool.findById(paramAsInt("id"))
    backend.delete()
    render(map("msg", "success"))
  }

  @Action(
    summary = "list templte", description = ""
  )
  @Parameters(Array(
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/ecs/list"), types = Array(GET, POST))
  def ecsList = {
    val items = EcsResourcePool.items()
    render(render(items))
  }

  @Action(
    summary = "add montior", description = "the monitor tells mlsql-cluster which tag are enabled with dynamic resource allocate"
  )
  @Parameters(Array(
    new Parameter(name = "tag", required = true, description = "tag the server", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "name", required = true, description = "the name of this row", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "minInstances", required = true, description = "the min number of instances", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "maxInstances", required = true, description = "the max number of instances", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "allocateType", required = true, description = "allocateType: local/cluster", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "allocateStrategy", required = true, description = "allocateStrategy, for now only JobNumAwareAllocate is supported", `type` = "string", allowEmptyValue = false)
  ))
  @At(path = Array("/monitor/add"), types = Array(GET, POST))
  def monitorAdd = {
    ElasticMonitor.requiredFields().asScala.foreach(item => require(hasParam(item), s"${item} is required"))
    ElasticMonitor.newOne(params(), true)
    render(map("msg", "success"))
  }

  @Action(
    summary = "remove montior", description = ""
  )
  @Parameters(Array(
    new Parameter(name = "id", required = true, description = "the id of montior", `type` = "string", allowEmptyValue = false)
  ))
  @At(path = Array("/monitor/remove"), types = Array(GET, POST))
  def monitorRemove = {
    ElasticMonitor.findById(paramAsInt("id")).delete()
    render(map("msg", "success"))
  }
}

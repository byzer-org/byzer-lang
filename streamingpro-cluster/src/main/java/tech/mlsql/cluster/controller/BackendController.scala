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
import net.liftweb.json.NoTypeHints
import net.liftweb.{json => SJSon}
import tech.mlsql.cluster.model.Backend
import tech.mlsql.cluster.service.BackendService
import tech.mlsql.common.utils.serder.json.JSONTool

import scala.collection.JavaConverters._

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
@OpenAPIDefinition(
  info = new BasicInfo(
    desc = "The collection of rest api are used to manager proxy backends.",
    state = State.alpha,
    contact = new Contact(url = "https://github.com/allwefantasy", name = "WilliamZhu", email = "allwefantasy@gmail.com"),
    license = new License(name = "Apache-2.0", url = "https://github.com/allwefantasy/streamingpro/blob/master/LICENSE")),
  externalDocs = new ExternalDocumentation(description =
    """

    """),
  servers = Array()
)
class BackendController extends ApplicationController {

  @Action(
    summary = "add backend", description = "backend is information about one mlsql instance. " +
      "mslql-cluster will decide how to proxy according to these backends."
  )
  @Parameters(Array(
    new Parameter(name = "url", required = true, description = "host:port format.", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "tag", required = true, description = "tags of this backend", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "name", required = true, description = "name of this backend", `type` = "string", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/backend/add"), types = Array(GET, POST))
  def backendAdd = {
    List("url", "tag", "name").foreach(item => require(hasParam(item), s"${item} is required"))
    if (Backend.findByName(param("name")) != null) {
      render(400, s"the name ${param("name")} have be taken")
    }
    val newParams = params().asScala.filterNot(f => f._1.startsWith("context.") || f._1.startsWith("action")).toMap.asJava
    Backend.newOne(newParams, true)
    BackendService.refreshCache
    render(map("msg", "success"))
  }

  @At(path = Array("/backend/list/names"), types = Array(GET, POST))
  def backendListNames = {
    import scala.collection.JavaConverters._
    val res = param("names").split(",").map { f =>
      Backend.findByName(f)
    }.filterNot(f => f == null).toList.asJava

    render(res)
  }

  @At(path = Array("/backend/name/check"), types = Array(GET, POST))
  def backendNameCheck = {
    val exists = Backend.findByName(param("name")) != null
    scalaRender(200, Map("msg" -> exists))
  }

  @Action(
    summary = "update tag of backend", description = ""
  )
  @Parameters(Array(
    new Parameter(name = "id", required = true, description = "the id of backend", `type` = "integer", allowEmptyValue = false),
    new Parameter(name = "merge", required = true, description = "overwrite all exists tag or append new default: overwrite", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "tag", required = true, description = "", `type` = "string", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/backend/tags/update"), types = Array(GET, POST))
  def backendTagsUpdate = {
    val backend = if (hasParam("id")) Backend.findById(paramAsInt("id")) else Backend.findByName(param("name"))
    if (param("merge", "overwrite") == "overwrite") {
      backend.attr("tag", param("tags"))
    } else {
      val newTags = backend.getTag.split(",").toSet ++ param("tags").split(",").toSet
      backend.attr("tag", newTags.mkString(","))
    }
    backend.save()
    BackendService.refreshCache
    render(map("msg", "success"))
  }

  @Action(
    summary = "remove backend", description = ""
  )
  @Parameters(Array(
    new Parameter(name = "id", required = true, description = "the id of backend", `type` = "integer", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/backend/remove"), types = Array(GET, POST))
  def backendRemove = {
    val backend = if (hasParam("id")) Backend.findById(paramAsInt("id")) else Backend.findByName(param("name"))
    backend.delete()
    BackendService.refreshCache
    render(map("msg", "success"))
  }

  @Action(
    summary = "list all backends", description = ""
  )
  @Parameters(Array())
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/backend/list"), types = Array(GET, POST))
  def backendList = {
    val items = Backend.items()
    require(hasParam("tag"), "filter tag is required")
    val tags = param("tag").split(",")
    val res = items.asScala.filter(f => f.getTags.toSet.intersect(tags.toSet).size > 0).asJava
    render(res)
  }

  @Action(
    summary = "find all backends is working(requests are executing)", description = ""
  )
  @Parameters(Array())
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/backend/active"), types = Array(GET, POST))
  def activeBackend = {
    implicit val formats = SJSon.Serialization.formats(NoTypeHints)
    render(SJSon.Serialization.write(BackendService.activeBackend.map(f => (f._1.getName, f._2))))
  }

  def scalaRender(status: Int, obj: AnyRef) = {
    render(status, JSONTool.toJsonStr(obj))
  }
}

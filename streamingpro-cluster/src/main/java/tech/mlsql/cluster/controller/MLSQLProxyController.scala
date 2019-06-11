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
import net.csdn.modules.transport.HttpTransportService.SResponse
import tech.mlsql.cluster.service.BackendService
import tech.mlsql.cluster.service.BackendService.mapSResponseToObject

import scala.collection.JavaConverters._

/**
  * 2018-12-04 WilliamZhu(allwefantasy@gmail.com)
  */
@OpenAPIDefinition(
  info = new BasicInfo(
    desc = "The collection of rest api are used to execute SQL, manager jobs and download hdfs file.",
    state = State.alpha,
    contact = new Contact(url = "https://github.com/allwefantasy", name = "WilliamZhu", email = "allwefantasy@gmail.com"),
    license = new License(name = "Apache-2.0", url = "https://github.com/allwefantasy/streamingpro/blob/master/LICENSE")),
  externalDocs = new ExternalDocumentation(description =
    """

    """),
  servers = Array()
)
class MLSQLProxyController extends ApplicationController {
  @Action(
    summary = "used to execute MLSQL script", description = "async/sync supports"
  )
  @Parameters(Array(
    new Parameter(name = "sql", required = true, description = "MLSQL script content", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "owner", required = false,
      description = "the user who execute this API and also will be used in MLSQL script automatically. " +
        "default: admin. Please set this owner properly.",
      `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "jobType", required = false, description = "script|stream|sql; default is script", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "jobName", required = false, description = "give the job you submit a name. uuid is ok.", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "timeout", required = false, description = "set timeout value for your job. default is -1 which means it is never timeout. millisecond", `type` = "int", allowEmptyValue = false),
    new Parameter(name = "silence", required = false, description = "the last sql in the script will return nothing. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "sessionPerUser", required = false, description = "If set true, the owner will have their own session otherwise share the same. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "async", required = false, description = "If set true ,please also provide a callback url use `callback` parameter and the job will run in background and the API will return.  default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "callback", required = false, description = "Used when async is set true. callback is a url. default: false", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "skipInclude", required = false, description = "disable include statement. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "tags", required = false, description = "proxy parameter,filter backend with this tags", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "proxyStrategy", required = false, description = "proxy parameter,How to choose backend, for now  supports: ResourceAwareStrategy|JobNumAwareStrategy|AllBackendsStrategy, default JobNumAwareStrategy", `type` = "string", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/run/script"), types = Array(GET, POST))
  def runScript = {
    if (!hasParam("tags") || param("tags", "").isEmpty) {
      throw new RuntimeException("tags is required")
    }
    val tags = param("tags", "")
    //FreeCoreBackendStrategy|TaskLessBackendStrategy
    val proxyStrategy = param("proxyStrategy", "JobNumAwareStrategy")
    val res = BackendService.execute(instance => {
      instance.runScript(params().asScala.toMap)
    }, tags, proxyStrategy)
    renderResult(tags, res)
  } 

  def renderResult(tags: String, res: Seq[Option[SResponse]]) = {
    if (res.size == 0) {
      render(500, map("msg", s"There are no backend with tags [${tags}] found."))
    }

    if (res.size == 1) {
      if (!res(0).isDefined) {
        render(500,"""{"msg":"No response from backend. Make sure all engines configured properly"}""")
      }
      if (res(0).get.getStatus == 200) {
        res(0).get.jsonStr match {
          case Some(i) => render(i)
          case None => render(res(0).get.getStatus, map("msg", res(0).get.getContent))
        }
      } else {
        render(res(0).get.getStatus, map("msg", res(0).get.getContent))
      }
    }
    val response = res.map { item => item.map(f => f.jsonStr).getOrElse("[]") }
    render("[" + response.mkString(",") + "]")
  }

  @At(path = Array("/run/sql"), types = Array(GET, POST))
  def runSQL = {
    val tags = param("tags", "")
    val proxyStrategy = param("proxyStrategy", "FreeCoreBackendStrategy")
    val res = BackendService.execute(instance => {
      instance.runSQL(params().asScala.toMap)
    }, param("tags", ""), proxyStrategy)
    renderResult(tags, res)
  }


}

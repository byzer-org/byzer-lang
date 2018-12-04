package tech.mlsql.cluster.controller

import net.csdn.annotation.rest._
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method.{GET, POST}
import net.liftweb.json.NoTypeHints
import net.liftweb.{json => SJSon}
import tech.mlsql.cluster.model.Backend
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
class APIController extends ApplicationController {
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
    new Parameter(name = "skipAuth", required = false, description = "disable table authorize . default: true", `type` = "boolean", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/run/script"), types = Array(GET, POST))
  def runScript = {
    val tags = param("tags", "")
    val res = BackendService.execute(instance => {
      instance.runScript(params().asScala.toMap)
    }, tags)
    if (!res.isDefined) {
      render(500, map("msg", s"There are no backend with tags [${tags}]"))
    }
    res.get.jsonStr match {
      case Some(i) => render(i)
      case None => render(500, map("msg", "backend error"))
    }
  }

  @At(path = Array("/run/sql"), types = Array(GET, POST))
  def runSQL = {
    val tags = param("tags", "")
    val res = BackendService.execute(instance => {
      instance.runSQL(params().asScala.toMap)
    }, param("tags", ""))
    if (!res.isDefined) {
      render(500, map("msg", s"There are no backend with tags [${tags}]"))
    }
    res.get.jsonStr match {
      case Some(i) => render(i)
      case None => render(500, map("msg", "backend error"))
    }
  }

  @At(path = Array("/backend/add"), types = Array(GET, POST))
  def backendAdd = {
    Backend.newBackend(params())
    render(map("msg", "success"))
  }

  @At(path = Array("/backend/list"), types = Array(GET, POST))
  def backendList = {
    render(Backend.items())
  }

  @At(path = Array("/backend/active"), types = Array(GET, POST))
  def activeBackend = {
    implicit val formats = SJSon.Serialization.formats(NoTypeHints)
    render(SJSon.Serialization.write(BackendService.activeBackend.map(f => (f._1.getName, f._2))))
  }
}

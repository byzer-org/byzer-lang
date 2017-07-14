package streaming.rest

import java.io.{File, FileOutputStream}

import net.csdn.annotation.rest.{At, BasicInfo, State}
import net.csdn.common.collections.WowCollections
import net.csdn.modules.http.{ApplicationController, RestRequest}
import net.csdn.modules.http.RestRequest.Method._
import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.fileupload.disk.DiskFileItemFactory
import org.apache.commons.fileupload.servlet.ServletFileUpload
import org.apache.commons.io.{FileUtils, FilenameUtils, IOUtils}
import streaming.db._
import streaming.bean.{ComplexParameterProcessor, DeployParameterService, Parameter}
import streaming.form.FormHelper
import streaming.service.{Scheduler, YarnApplicationState, YarnRestService}
import streaming.shell.ShellCommand

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 12/7/2017.
  */
class RestController extends ApplicationController {

  DB

  def view(obj: AnyRef) = {
    JSONArray.fromObject(DeployParameterService.toStr(obj))
  }


  @At(path = Array("/spark_monitor.html"), types = Array(GET, POST))
  def spark_monitor = {
    val command = param("command", "start")
    TSparkApplication.find(param("id").toLong) match {
      case Some(app) =>
        app.keepRunning = if (command == "start") TSparkApplication.KEEP_RUNNING else TSparkApplication.NO_KEEP_RUNNING
        app.watchInterval = if (command == "start") TSparkApplication.WATCH_INTERVAL else TSparkApplication.NO_WATCH_INTERVAL
        TSparkApplication.reSave(app)
      case None =>
    }

    redirectTo("/jobs.html", WowCollections.map())
  }

  @At(path = Array("/remove_job.html"), types = Array(GET, POST))
  def remove_job = {

    val app = TSparkApplication.find(param("id").toLong).get
    if (app.applicationId == null || app.applicationId.isEmpty || !YarnRestService.isRunning(app.url, app.applicationId)) {
      TSparkApplication.delete(app.id)
    }
    redirectTo("/jobs.html", WowCollections.map())
  }

  @At(path = Array("/submit_job_index.html"), types = Array(GET, POST))
  def submit_job_index = {

    val parameter = Parameter(
      name = "mmspark.jars",
      parameterType = "string",
      app = "jar",
      desc = "",
      label = "依赖jar包勾选",
      priority = 0,
      formType = "checkbox",
      actionType = "checkbox",
      comment = "", value = "")

    val jarDependencies = FormHelper.formatFormItem(new ComplexParameterProcessor().process(parameter)).value

    val appParameters = DeployParameterService.
      installSteps("spark").map(f => f.priority).distinct.sortBy(f => f).map(f => DeployParameterService.installStep("spark", f).map(f => FormHelper.formatFormItem(f)))

    val jarPathMessage = if (isEmpty(param("jarPath"))) "" else s" jar is uploaded : ${param("jarPath")}"
    renderHtml(200, "/rest/submit_job.vm", Map("params" -> view(List(
      Map("name" -> "StreamingPro配置", "value" -> appParameters(0)),
      Map("name" -> "资源配置", "value" -> appParameters(2)),
      Map("name" -> "Spark参数配置", "value" -> appParameters(1))
    )), "jarDependencies" -> jarDependencies)
    )
  }

  @At(path = Array("/submit_job.html"), types = Array(GET, POST))
  def submit_job = {
    val app = if (param("id") != null) {
      TSparkApplication.find(paramAsLong("id", -1)).get
    } else {
      TParamsConf.save(params().toMap)
      new SparkSubmitCommand().process(params().toMap)
    }

    val (taskId, host) = Scheduler.submitApp(app)
    redirectTo("/process.html", Map("taskId" -> taskId, "appId" -> app.id))
  }

  @At(path = Array("/job_history.html"), types = Array(GET, POST))
  def job_history = {

  }

  @At(path = Array("/jobs.html"), types = Array(GET, POST))
  def jobs = {
    val sparkApps = TSparkApplication.list

    val result = sparkApps.map { sparkApp =>

      val startOperate = s""" <a href="/submit_job.html?id=${sparkApp.id}">启动</a>  """

      val deleteOperate = s""" <a href="/remove_job.html?id=${sparkApp.id}">删除该信息</a>  """

      val className = sparkApp.source.split("--name").last.trim.split("\\s+").head

      val watch = TSparkApplication.shouldWatch(sparkApp)

      val basicInfo = Map("yarnUrl" -> sparkApp.url, "watch" -> watch, "className" -> className, "app" -> sparkApp, "state" -> "FAIL")

      val items = if (sparkApp.applicationId == null || sparkApp.applicationId.isEmpty) null else YarnRestService.findApp(sparkApp.url, sparkApp.applicationId)

      if (items == null || items.isEmpty) {
        logger.info(s"sparkApp.applicationId=${sparkApp.applicationId} not exits")
        val operate = Map("startOperate" -> startOperate,
          "deleteOperate" -> deleteOperate)

        Map(
          "running" -> false,
          "info" -> Map()
        ) ++ basicInfo ++ operate

      }
      else {
        val info = items(0)
        val running = YarnRestService.isRunning(items)
        logger.info(s"sparkApp.applicationId=${sparkApp.applicationId} is running=${running}")

        val operate = if (running) Map()
        else Map("startOperate" -> startOperate,
          "deleteOperate" -> deleteOperate)


        Map(
          "running" -> running,
          "info" -> info

        ) ++ basicInfo ++ operate ++ Map("state" -> info.state)
      }


    }
    renderHtml(200, "/rest/jobs.vm", Map("result" -> view(result)))
  }

  @At(path = Array("/upload.html"), types = Array(RestRequest.Method.GET, RestRequest.Method.POST))
  def upload = {
    renderHtml(200, "/rest/upload.vm", WowCollections.map())
  }

  @At(path = Array("/process.html"), types = Array(RestRequest.Method.GET, RestRequest.Method.POST))
  def process = {
    val taskId = param("taskId")
    val app = TSparkApplication.find(paramAsLong("appId", -1)).get
    val content = "Spark 提交参数为：\n" + app.source + "\n\n" + ShellCommand.exec("cat /tmp/mammuthus/" + taskId + "/stderr") + ShellCommand.exec("cat /tmp/mammuthus/" + taskId + "/stdout") //ShellCommand.readFile("/tmp/mammuthus/" + taskId, paramAsLong("offset", 0), paramAsLong("readSize", 1024))
    renderHtml(200, "/rest/process.vm", Map("content" -> content, "taskId" -> taskId))
  }

  @At(path = Array("/form/upload"), types = Array(RestRequest.Method.GET, RestRequest.Method.POST))
  @BasicInfo(
    desc = "可指定哪些服务器下载指定url地址的文件到指定目录",
    state = State.alpha,
    testParams = "",
    testResult = "task submit",
    author = "WilliamZhu",
    email = "allwefantasy@gmail.com"
  )
  def formUpload = {
    val items = new ServletFileUpload(new DiskFileItemFactory()).parseRequest(request.httpServletRequest())
    var jarPath: File = null
    try {
      items.filterNot(f => f.isFormField).map {
        item =>
          //val fieldName = item.getFieldName();
          val fileName = FilenameUtils.getName(item.getName())
          val fileContent = item.getInputStream()
          val targetPath = new File(param("path", "/tmp/upload/") + fileName)
          jarPath = targetPath
          logger.info(s"upload to ${targetPath.getPath}")
          FileUtils.copyInputStreamToFile(fileContent, targetPath)
          TSparkJar.findByName(fileName) match {
            case Some(i) =>
            case None => TSparkJar.save(new TSparkJar(0, fileName, targetPath.getPath, System.currentTimeMillis()))
          }
      }
    } catch {
      case e: Exception =>
        logger.info("upload fail ", e)
        render(500, s"upload fail,check master log ${e.getMessage}")
    }
    val fields = items.filter(f => f.isFormField && f.getFieldName == "redirect")
    val redirect = if (fields.size == 0) "-" else fields.head.getString
    if (redirect == "-") render("upload success") else redirectTo(redirect, Map("jarPath" -> jarPath.getPath))
  }

}

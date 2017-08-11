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
import streaming.bean.{ComplexParameterProcessor, DeployParameterService}
import streaming.form.HtmlHelper
import streaming.helper.rest.RestHelper
import streaming.service.{MonitorScheduler, YarnApplicationState, YarnRestService}
import streaming.shell.ShellCommand

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 12/7/2017.
  */
class RestController extends ApplicationController {

  DB

  @At(path = Array("/test.html"), types = Array(GET))
  def test_index = {
    renderHtml(200, "/rest/test.vm", WowCollections.map())
  }

  @At(path = Array("/parameters.html"), types = Array(GET))
  def parameters_index = {
    renderHtml(200, "/rest/parameters.vm", pv(Map()))
  }

  @At(path = Array("/query.html"), types = Array(GET))
  def query_index = {
    renderHtml(200, "/rest/query.vm", pv(Map("sparkSqlServer" -> ManagerConfiguration.sparkSqlServer)))
  }

  @At(path = Array("/query"), types = Array(GET))
  def query = {
  }

  @At(path = Array("/spark_monitor"), types = Array(GET, POST))
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

  @At(path = Array("/submit_job.html"), types = Array(GET, POST))
  def submit_job_index = {

    val parameter = TSparkJobParameter(
      id = -1,
      name = "mmspark.jars",
      parentName = "",
      parameterType = "string",
      app = "jar",
      description = "",
      label = "依赖jar包勾选",
      priority = 0,
      formType = "checkbox",
      actionType = "checkbox",
      comment = "",
      value = "")

    val jarDependencies = new RestHelper().formatFormItem(new ComplexParameterProcessor().process(parameter)).value

    val appParameters = DeployParameterService.
      process.map(f => (f.priority, f)).
      groupBy(f => f._1).
      map(f => (f._1, f._2.map(f => f._2))).toList.
      sortBy(f => f._1)
      .map(f => f._2.map(f => new RestHelper().formatFormItem(f)))

    renderHtml(200, "/rest/submit_job.vm",
      pv(Map("params" -> view(List(
        Map("name" -> "StreamingPro配置", "value" -> appParameters(0)),
        Map("name" -> "资源配置", "value" -> appParameters(2)),
        Map("name" -> "Spark参数配置", "value" -> appParameters(1))
      )), "jarDependencies" -> jarDependencies))
    )
  }

  @At(path = Array("/submit_job"), types = Array(GET, POST))
  def submit_job = {
    val app = if (param("id") != null) {
      TSparkApplication.find(paramAsLong("id", -1)).get
    } else {
      TParamsConf.save(params().toMap)
      new SparkSubmitCommand().process(params().toMap)
    }

    val (taskId, host) = MonitorScheduler.submitApp(app)
    redirectTo("/process.html", pv(Map("taskId" -> taskId, "appId" -> app.id)))
  }

  @At(path = Array("/job_history.html"), types = Array(GET, POST))
  def job_history = {

  }

  @At(path = Array("/jobs.html"), types = Array(GET, POST))
  def jobs = {
    var sparkApps = TSparkApplication.list
    if (!isEmpty(param("search"))) {
      sparkApps = sparkApps.filter(f => f.source.contains(param("search")))
    }

    val result = sparkApps.zipWithIndex.map { case (sparkApp, index) =>

      val rowBuffer = new ArrayBuffer[String]()
      rowBuffer += index.toString

      val yarnAppList = if (isEmpty(sparkApp.applicationId)) None else Some(YarnRestService.findApp(sparkApp.url, sparkApp.applicationId))

      val yarnApp = yarnAppList match {
        case Some(i) => if (i.size == 0) None else Some(i(0))
        case None => None
      }

      rowBuffer += HtmlHelper.link(url = s"http://${sparkApp.url}/cluster/app/${sparkApp.applicationId}", name = sparkApp.applicationId)

      rowBuffer += HtmlHelper.link(url = s"http://${sparkApp.url}/cluster/app/${sparkApp.parentApplicationId}", name = sparkApp.parentApplicationId)

      val appName = sparkApp.source.split("--name").last.trim.split("\\s+").head
      rowBuffer += appName

      def stateStyleMapping(state: String) = {
        YarnApplicationState.withName(state) match {
          case YarnApplicationState.FINISHED => "btn-info"
          case YarnApplicationState.RUNNING => "btn-success"
          case YarnApplicationState.FAILED | YarnApplicationState.KILLED => "btn-danger"
          case _ => "btn-secondary"
        }

      }

      val state = yarnApp match {
        case None => HtmlHelper.button("LOST", "btn-danger")
        case Some(ya) =>
          HtmlHelper.button(ya.state, stateStyleMapping(ya.state))
      }

      rowBuffer += state

      val startOperate = if (yarnApp.map(f => f.state).mkString("") != YarnApplicationState.RUNNING.toString) HtmlHelper.link(url = s"/submit_job?id=${sparkApp.id}", name = "启动")
      else ""

      rowBuffer += startOperate


      val watch = TSparkApplication.shouldWatch(sparkApp)
      rowBuffer += (if (watch) HtmlHelper.button("已监控", "btn-success")
      else HtmlHelper.button("未监控", "btn-danger"))

      rowBuffer += (if (watch) HtmlHelper.link(s"/spark_monitor?command=stop&id=${sparkApp.id}", "取消监控")
      else if (isEmpty(sparkApp.applicationId)) "" else HtmlHelper.link(s"/spark_monitor?command=start&id=${sparkApp.id}", "监控"))

      val deleteOperate = if (yarnApp.map(f => f.state).mkString("") != YarnApplicationState.RUNNING.toString) HtmlHelper.link(url = s"/remove_job.html?id=${sparkApp.id}", name = "删除信息")
      else ""
      rowBuffer += deleteOperate

      rowBuffer
    }
    renderHtml(200, "/rest/jobs.vm", pv(Map("result" -> view(result))))
  }

  @At(path = Array("/upload.html"), types = Array(RestRequest.Method.GET, RestRequest.Method.POST))
  def upload = {
    renderHtml(200, "/rest/upload.vm", pv(Map()))
  }

  @At(path = Array("/process.html"), types = Array(RestRequest.Method.GET, RestRequest.Method.POST))
  def process = {
    val taskId = param("taskId")
    val app = TSparkApplication.find(paramAsLong("appId", -1)).get
    val content = "Spark 提交参数为：\n" + app.source + "\n\n" +
      ShellCommand.exec("cat /tmp/mammuthus/" + taskId + "/stderr") +
      ShellCommand.exec("cat /tmp/mammuthus/" + taskId + "/stdout") //ShellCommand.readFile("/tmp/mammuthus/" + taskId, paramAsLong("offset", 0), paramAsLong("readSize", 1024))
    renderHtml(200, "/rest/process.vm", pv(Map("content" -> content, "taskId" -> taskId)))
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

  def nav() = {
    //val mapping = Map("/jobs.html" -> "任务管理", "/submit_job.html" -> "提交任务", "/upload.html" -> "Jar包上传")
    val navBuffer = new ArrayBuffer[String]()

    def active(path: String) = {
      if (request.path == path) {
        " active "
      }
      else {
        " "
      }
    }

    navBuffer += HtmlHelper.link(url = "/submit_job.html", name = "提交任务", style = active("/submit_job.html"))
    navBuffer += HtmlHelper.link(url = "/jobs.html", name = "任务管理", style = active("/jobs.html"))
    navBuffer += HtmlHelper.link(url = "/upload.html", name = "Jar包上传", style = active("/upload.html"))
    navBuffer += HtmlHelper.link(url = "/query.html", name = "Spark SQL Server", style = active("/query.html"))
    navBuffer
  }

  def view(obj: AnyRef) = {
    JSONArray.fromObject(DeployParameterService.toStr(obj))
  }

  def pv(item: Map[Any, Any]) = {
    Map("nav" -> view(nav())) ++ item
  }

}

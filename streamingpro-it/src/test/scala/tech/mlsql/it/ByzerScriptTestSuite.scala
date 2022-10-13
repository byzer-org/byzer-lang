package tech.mlsql.it

import net.csdn.modules.transport.DefaultHttpTransportService
import org.apache.http.HttpEntity
import org.apache.http.util.EntityUtils
import tech.mlsql.crawler.RestUtils
import tech.mlsql.it.contiainer.ByzerCluster
import tech.mlsql.it.utils.DockerUtils
import tech.mlsql.it.utils.DockerUtils.getCurProjectRootPath
import tech.mlsql.runtime.VersionRangeChecker

import java.io.File
import java.util.UUID
import scala.collection.mutable

/**
 * 23/02/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class ByzerScriptTestSuite extends LocalBaseTestSuite {
  val version: String = DockerUtils.getSparkVersion
  var url: String = ""
  var initialByzerPlugins: Seq[String] = Seq()
  private var cluster: ByzerCluster = _
  val curTestManager = new TestManager()
  var curTestCaseDirPath: Seq[String] = _
  var curDataDirPath: String = _

  override def getTestManager: TestManager = curTestManager

  override def getTestCaseDirPath: Seq[String] = curTestCaseDirPath

  override def getDataDirPath: String = curDataDirPath

  def setupCluster(dataDirPath: String): ByzerCluster = {
    cluster = ByzerCluster.forSpec(dataDirPath)
    cluster.start()
    cluster
  }

  override def afterAll(): Unit = {
    println("The ByzerScriptTestSuite integration test is complete, and a graceful shutdown is performed...")
    if (cluster != null) {
      cluster.stop()
      cluster = null
    }
  }

  override def beforeAll(): Unit = {
    println("Initialize ByzerScriptTestSuite configuration before integration test execution...")
    curTestCaseDirPath.foreach(dirPath => {
      curTestManager.loadTestCase(new File(dirPath))
    })
    initPlugins()
  }

  override def initPlugins(): Unit = {
    // no-op
  }

  def runScript(url: String, user: String, code: String, callbackHeader: String = ""): (Int, String) = {
    val jobName = UUID.randomUUID().toString
    val params = mutable.Map("sql" -> code, "owner" -> user,
      "jobName" -> jobName, "sessionPerUser" -> "true", "sessionPerRequest" -> "true")
    if (callbackHeader != "") params.put("callbackHeader", callbackHeader)
    logInfo(s"The test submits a script to the container through Rest, url:$url, sql:$code")
    val (status, result) = RestUtils.rest_request_string(url, "post", params.toMap,
      Map("Content-Type" -> "application/x-www-form-urlencoded"), Map("socket-timeout" -> "1800s",
        "connect-timeout" -> "1800s", "retry" -> "1")
    )
    logInfo(s"status:$status,result:$result")
    (status, result)
  }

  def runScriptWithHeader(url: String, user: String, code: String, callbackHeader: String = ""): (Int, HttpEntity) = {
    val jobName = UUID.randomUUID().toString
    val params = mutable.Map("sql" -> code, "owner" -> user,
      "jobName" -> jobName, "sessionPerUser" -> "true", "sessionPerRequest" -> "true")
    if (callbackHeader != "") params.put("callbackHeader", callbackHeader)
    logInfo(s"The test submits a script to the container through Rest, url:$url, sql:$code")
    val (status, result) = RestUtils.rest_request(url, "post", params.toMap,
      Map("Content-Type" -> "application/x-www-form-urlencoded"), Map("socket-timeout" -> "1800s",
        "connect-timeout" -> "1800s", "retry" -> "1")
    )
    logInfo(s"status:$status,result:$result")
    (status, result)
  }

  override def setupRunParams(): Unit = {
    val path = getCurProjectRootPath
    curTestCaseDirPath = Seq(
      path + "src/test/resources/sql/yarn_mode",
      path + "src/test/resources/sql/all_mode"
    )
    curDataDirPath = path + "src/test/resources/data"
  }

  def beforeInitContainers(): Unit = {
    setupRunParams()
  }

  beforeInitContainers()

  if (VersionRangeChecker.isVersionCompatible(">=3.0.0", version)) {

    println("Current spark version is 3.X, step to javaContainer test...")
    val cluster: ByzerCluster = setupCluster(curDataDirPath)
    val hadoopContainer = cluster.hadoopContainer
    val byzerLangContainer = cluster.byzerLangContainer
    val javaContainer = byzerLangContainer.container
    url = s"http://${javaContainer.getHost}:${javaContainer.getMappedPort(9003)}/run/script"

    test("javaContainer") {
      runScript(url, user, "select 1 as a,'jack' as b as bbc;")
    }

    test("Execute yarn sql file") {
      try {
        val (_, result) = runScriptWithHeader(url, user, "select 1 as a,'jack' as b as bbc;",
          """{"Authorization":"Bearer acc"}""")
        val _result = EntityUtils.toString(result, DefaultHttpTransportService.charset)
        println("With callbackHeader result:" + _result)
        assert(_result === "[{\"a\":1,\"b\":\"jack\"}]")
      } catch {
        case _: Exception =>
          val res = "callbackHeader should be returned normally in the byzer callback!"
          logError(res)
          throw new RuntimeException(res)
      }

      curTestManager.testCases.foreach(testCase => {
        try {
          val (status, result) = runScript(url, user, testCase.sql)
          curTestManager.acceptRest(testCase, status, result, null)
        } catch {
          case e: Exception =>
            curTestManager.acceptRest(testCase, 500, null, e)
        }
      })
      curTestManager.report()
    }

  } else {
    logInfo(s"Can not support current version:$version, skip it.")
  }

}

package tech.mlsql.it

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.crawler.RestUtils
import tech.mlsql.it.contiainer.ByzerCluster
import tech.mlsql.it.utils.DockerUtils
import tech.mlsql.it.utils.DockerUtils.getCurProjectRootPath

import java.io.File
import java.util.UUID

/**
 * 23/02/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class ByzerScriptTestSuite extends LocalBaseTestSuite with Logging {

  val version: String = DockerUtils.getSparkShortVersion
  var url: String = ""
  private var cluster: ByzerCluster = _
  var initialByzerPlugins: Seq[String] = Seq()

  def setupCluster(): ByzerCluster = {
    cluster = ByzerCluster.forSpec()
    cluster.start()
    cluster
  }

  override def afterAll(): Unit = {
    println("The integration test is complete, and a graceful shutdown is performed...")
    if (cluster != null) {
      cluster.stop()
      cluster = null
    }
  }

  override def copyDataToUserHome(user: String): Unit = {
    // no-op
  }

  override def initPlugins(): Unit = {
    // set plugin context loader
    Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader)
    initialByzerPlugins.foreach(name => {
      runScript(url, user, s"""!plugin app remove "$name-$version";""")
      runScript(url, user, s"""!plugin app add - "$name-$version";""")
    })
  }

  def runScript(url: String, user: String, code: String): (Int, String) = {
    val jobName = UUID.randomUUID().toString
    logInfo(s"The test submits a script to the container through Rest, url:$url, sql:$code")
    val (status, result) = RestUtils.rest_request_string(url, "post", Map("sql" -> code, "owner" -> user, "jobName" -> jobName),
      Map("Content-Type" -> "application/x-www-form-urlencoded"), Map("socket-timeout" -> "180s", "connect-timeout" -> "180s")
    )
    logInfo(s"status:$status,result:$result")
    (status, result)
  }

  override def setupRunParams(): Unit = {
    val path = getCurProjectRootPath
    testCaseDirPath = path + "/src/test/resources/sql/yarn"
    dataDirPath = path + "/src/test/resources/data"
  }

  override def beforeAll(): Unit = {
    println("Initialize configuration before integration test execution...")
    setupWorkingDirectory()
    setupRunParams()
    copyDataToUserHome(user)
    TestManager.loadTestCase(new File(testCaseDirPath))
    initPlugins()
  }

  def before(): Unit = {
    // no-op
  }

  if ("3.0".equals(version)) {
    before()
    println("Current spark version is 3.0, step to javaContainer test...")
    val cluster: ByzerCluster = setupCluster()
    val hadoopContainer = cluster.hadoopContainer
    val byzerLangContainer = cluster.byzerLangContainer
    val javaContainer = cluster.byzerLangContainer.container
    url = s"http://${javaContainer.getHost}:${javaContainer.getMappedPort(9003)}/run/script"

    test("javaContainer") {
      // 9870, 8088, 19888, 10002, 8042
      println("Current hadoop ui port(8088) is :" + hadoopContainer.container.getMappedPort(8088))
      println("Current containerlogs ui port(8042) is :" + hadoopContainer.container.getMappedPort(8042))
      println("Current hdfs ui port is :" + hadoopContainer.container.getMappedPort(9870))
      println("Current jobhistory ui port is :" + hadoopContainer.container.getMappedPort(19888))
      println("Current spark xdebug(10002) port is :" + hadoopContainer.container.getMappedPort(10002))
      // 9003, 4040, 8265, 10002
      println("Current byzer ui port is :" + javaContainer.getMappedPort(9003))
      println("Current spark ui port is :" + javaContainer.getMappedPort(4040))
      println("Current ray dashboard port is :" + javaContainer.getMappedPort(8265))
      println("Current ray head port is :" + javaContainer.getMappedPort(10002))
      runScript(url, user, "select 1 as a,'jack' as b as bbc;")
    }

    test("Execute yarn sql file") {
      TestManager.testCases.foreach(testCase => {
        try {
          val (status, result) = runScript(url, user, testCase.sql)
          TestManager.acceptRest(testCase, status, result, null)
        } catch {
          case e: Exception =>
            TestManager.acceptRest(testCase, 500, null, e)
        }
      })

      TestManager.report()
    }

  } else {
    logInfo(s"Can not support current version:$version, skip it.")
  }

}

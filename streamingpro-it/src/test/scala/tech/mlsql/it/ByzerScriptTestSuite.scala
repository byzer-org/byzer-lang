package tech.mlsql.it

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.crawler.udf.FunctionsUtils
import tech.mlsql.it.contiainer.ByzerCluster
import tech.mlsql.it.utils.DockerUtils

import java.util.UUID

/**
 * 23/02/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class ByzerScriptTestSuite extends FlatSpec with Suite with Matchers with BeforeAndAfterAll with Logging {

  val version: String = DockerUtils.getSparkShortVersion
  private var cluster: ByzerCluster = _

  def setupCluster(): ByzerCluster = {
    cluster = ByzerCluster.forSpec()
    cluster.start()
    cluster
  }

  override def afterAll(): Unit = {
    if (cluster != null) {
      cluster.stop()
      cluster = null
    }
  }

  override def beforeAll(): Unit = {
    println("---------------beforeAll---------------")
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

    "javaContainer" should "retrieve non-0 port for any of services" in {
      println(hadoopContainer.container.getMappedPort(8088))
      println(javaContainer.getMappedPort(9003))
      val url = s"http://${javaContainer.getHost}:${javaContainer.getMappedPort(9003)}/run/script"
      val sql = "select 1 as a,'jack' as b as bbc;"
      val owner = "admin"
      val jobName = UUID.randomUUID().toString
      logInfo(s"The test submits a script to the container through Rest, url:$url, sql:$sql")
      val (status, result) = FunctionsUtils.rest_request(url, "post", Map("sql" -> sql, "owner" -> owner, "jobName" -> jobName),
        Map("Content-Type" -> "application/x-www-form-urlencoded"), Map("config.socket-timeout"->"180s","config.connect-timeout"->"180s")
      )
      logInfo(s"status:$status,result:$result")
    }
  } else {
    logInfo(s"Can not support current version:$version, skip it.")
  }

}

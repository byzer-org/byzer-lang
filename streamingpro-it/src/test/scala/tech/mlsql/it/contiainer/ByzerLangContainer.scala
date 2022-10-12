package tech.mlsql.it.contiainer

import tech.mlsql.common.utils.log.Logging
import tech.mlsql.it.utils.DockerUtils

/**
 * 23/02/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class ByzerLangContainer(clusterName: String, image: String) extends ChaosContainer(clusterName: String, image: String) with Logging {

  val BYZER_CONTAINER_HOME = "/home/deploy/byzer-lang/"

  def this(clusterName: String) {
    this(clusterName, ByzerLangContainer.DEFAULT_BYZER_IMAGE_NAME)
  }

  override def stop(): Unit = {
    if (CONTAINERS_LEAVE_RUNNING) {
      logWarning("Ignoring stop due to CONTAINERS_LEAVE_RUNNING=true.")
      return
    }
    super.stop()
  }

  override def start(): Unit = {
    beforeStart()
    super.start()
    afterStart()
  }

  override def afterStart(): Unit = {
    DockerUtils.runCommandAsyncWithLogging(container.getDockerClient, container.getContainerId,
      Seq("tail", "-f", BYZER_CONTAINER_HOME + "logs/byzer-lang.log"))
  }

  override def beforeStop(): Unit = {
    super.beforeStop()
    if (null != container.getContainerId) DockerUtils.dumpContainerDirToTargetCompressed(container.getDockerClient,
      container.getContainerId, BYZER_CONTAINER_HOME + "logs")
  }

}

object ByzerLangContainer {
  val DEFAULT_BYZER_IMAGE_NAME: String = System.getenv.getOrDefault("DEFAULT_BYZER_IMAGE_NAME",
    "byzer/byzer-lang:3.3.0-latest")
}

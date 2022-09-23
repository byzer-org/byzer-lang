package tech.mlsql.it.contiainer

/**
 * 23/02/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class HadoopContainer(clusterName: String, image: String) extends ChaosContainer(clusterName: String, image: String) {

  def this(clusterName: String) {
    this(clusterName, HadoopContainer.DEFAULT_HADOOP_IMAGE_NAME)
  }

  override def start(): Unit = {
    super.beforeStart()
    super.start()
  }

  override def beforeStop(): Unit = {
    super.beforeStop()
  }

  override def stop(): Unit = {
    if (CONTAINERS_LEAVE_RUNNING) {
      logWarning("Ignoring stop due to CONTAINERS_LEAVE_RUNNING=true.")
      return
    }
    super.stop()
  }

}

object HadoopContainer {
  val DEFAULT_HADOOP_IMAGE_NAME: String = System.getenv.getOrDefault("BYZER_HADOOP_IMAGE_NAME",
    "byzer/hadoop3:latest")
}
package tech.mlsql.it.contiainer

import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import com.dimafeng.testcontainers.GenericContainer.FileSystemBind
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.startupcheck.StartupCheckStrategy
import org.testcontainers.containers.wait.strategy.WaitStrategy
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.it.utils.DockerUtils

import java.util.function.Consumer
import java.util.{Objects, UUID}
import scala.util.Try

/**
 * 23/09/2022 hellozepp(lisheng.zhanglin@163.com)
 */
object MySQLContainer {
  val DEFAULT_MYSQL_CONTAINER_PORT = 3306
  val DEFAULT_MYSQL_PORT = 33306
  val DEFAULT_MYSQL_IMAGE_NAME: String = System.getenv.getOrDefault("MYSQL_IMAGE_NAME",
    "mysql:5.7")
}

/**
 * With FixedHostPortGenericContainer, a fixed port mysql container is defined, which can easily access a fixed port
 * mysql in static test sql, and access fixed IP mapping through network; in addition, we can customize more robust
 * log management and container life cycle manage.
 *
 */
class MySQLContainer(imageName: String,
                     exposedPorts: Seq[Int] = Seq(),
                     env: Map[String, String] = Map(),
                     command: Seq[String] = Seq(),
                     classpathResourceMapping: Seq[FileSystemBind] = Seq(),
                     waitStrategy: Option[WaitStrategy] = None,
                     exposedHostPort: Int,
                     exposedContainerPort: Int,
                     fileSystemBind: Seq[FileSystemBind] = Seq(),
                     startupCheckStrategy: Option[StartupCheckStrategy] = None,
                     clusterName: String
                    ) extends FixedHostPortGenericContainer(imageName, exposedPorts, env, command,
  classpathResourceMapping, waitStrategy, exposedHostPort, exposedContainerPort, fileSystemBind, startupCheckStrategy)
  with Logging {

  val CONTAINERS_LEAVE_RUNNING: Boolean = Try(System.getenv("BYZER_CONTAINERS_LEAVE_RUNNING"))
    .map(env => env.toBoolean).getOrElse(false)

  val _clusterName: String = clusterName

  override def start(): Unit = {
    beforeStart()
    super.start()
    afterStart()
  }

  def beforeStart(): Unit = {
    configureLeaveContainerRunning(this)
    tailContainerLog(this)
  }

  /**
   * Use this function to delete the container for which the test is done
   *
   * @param container GenericContainer
   */
  def configureLeaveContainerRunning(container: FixedHostPortGenericContainer): Unit = {
    // use Testcontainers reuse containers feature to leave the container running
    if (CONTAINERS_LEAVE_RUNNING) {
      container.configure { c =>
        c.addEnv("testcontainers.reuse.enable", "true")
        c.withReuse(true)
        // add label that can be used to find containers that are left running.
        c.withLabel("byzercontainer", "true")
        // add a random label to prevent reuse of containers
        c.withLabel("byzercontainer.random", UUID.randomUUID.toString)
      }
      container.container.addEnv("MALLOC_ARENA_MAX", "1")
    }
  }

  protected def tailContainerLog(container: MySQLContainer): Unit = {
    container.configure { c =>
      c.withLogConsumer(new Consumer[OutputFrame]() {
        def accept(item: OutputFrame): Unit = {
          logInfo(item.getUtf8String)
        }
      })
    }
  }

  def afterStart(): Unit = {
    // no-op
  }

  override def stop(): Unit = {
    beforeStop()
    super.stop()
  }

  protected def beforeStop(): Unit = {
    if (null != container.getContainerId) {
      // dump the container log
      DockerUtils.dumpContainerLogToTarget(container.getDockerClient, container.getContainerId)
    }
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[MySQLContainer]) return false
    val another = o.asInstanceOf[MySQLContainer]
    _clusterName == another._clusterName && super.equals(another)
  }

  override def hashCode: Int = 31 * super.hashCode + Objects.hash(clusterName)

}

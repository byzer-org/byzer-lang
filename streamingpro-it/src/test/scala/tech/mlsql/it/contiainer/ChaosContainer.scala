package tech.mlsql.it.contiainer

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.output.OutputFrame
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.it.utils.DockerUtils

import java.util.function.Consumer
import java.util.{Objects, UUID}
import scala.util.Try

/**
 * 23/02/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class ChaosContainer[SelfT <: ChaosContainer[SelfT]](clusterName: String, image: String) extends GenericContainer(image: String) with Logging {

  val CONTAINERS_LEAVE_RUNNING: Boolean = Try(System.getenv("BYZER_CONTAINERS_LEAVE_RUNNING"))
    .map(env => env.toBoolean).getOrElse(false)

  val _clusterName: String = clusterName
  val _image: String = image

  override def start(): Unit = {
    beforeStart()
    super.start()
    afterStart()
  }

  def beforeStart(): Unit = {
    configureLeaveContainerRunning(this)
    tailContainerLog(this)
  }

  def afterStart(): Unit = {
    // no-op
  }

  /**
   * Use this function to delete the container for which the test is done
   * @param container GenericContainer
   */
  def configureLeaveContainerRunning(container: GenericContainer): Unit = {
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

  protected def tailContainerLog(container: GenericContainer): Unit = {
    container.configure { c =>
      c.withLogConsumer(new Consumer[OutputFrame]() {
        def accept(item: OutputFrame): Unit = {
          logInfo(item.getUtf8String)
        }
      })
    }
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
    if (!o.isInstanceOf[ChaosContainer[SelfT]]) return false
    val another = o.asInstanceOf[ChaosContainer[SelfT]]
    _clusterName == another._clusterName && super.equals(another)
  }

  override def hashCode: Int = 31 * super.hashCode + Objects.hash(clusterName)

}

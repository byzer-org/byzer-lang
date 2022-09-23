package tech.mlsql.it.contiainer

import com.github.dockerjava.api.command.CreateContainerCmd
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.Network.NetworkImpl
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.it.utils.DockerUtils
import java.io.File
import java.time.Duration
import java.time.temporal.ChronoUnit.SECONDS
import java.util.function.Consumer
import scala.util.Random

import scala.sys.process._

/**
 * 24/02/2022 hellozepp(lisheng.zhanglin@163.com)
 */
class ByzerCluster(val byzerLangContainer: ByzerLangContainer, val hadoopContainer: HadoopContainer) extends Logging {

  def start(): Unit = {
    hadoopContainer.start()
    logInfo("Successfully started local hadoop container.")
    byzerLangContainer.start()
    logInfo("Successfully started local byzer-lang container.")
  }

  def stop(): Unit = {
    byzerLangContainer.stop()
    logInfo("Successfully stop local byzer-lang container.")
    if (byzerLangContainer.container != null && byzerLangContainer.container.getContainerId != null) {
      Thread.sleep(10000)
    }
    hadoopContainer.stop()
    logInfo("Successfully stop local hadoop container.")
  }

}

object ByzerCluster extends Logging {
  lazy private val network = NetworkImpl.builder.build
  private val clusterName = "byzer-it"
  private val networkAliases = "byzer-network"

  def forSpec(dataDirPath: String): ByzerCluster = {
    beforeAll()
    lazy val hadoopContainer: HadoopContainer = new HadoopContainer(clusterName).configure { c =>
      c.addExposedPorts(9870, 8088, 19888, 10002, 8042)
      c.withNetwork(network)
      c.withNetworkAliases(ByzerCluster.appendClusterName(networkAliases))
      c.setWaitStrategy(new HttpWaitStrategy()
        .forPort(8088).forPath("/cluster").forStatusCode(200)
        .withStartupTimeout(Duration.of(1500, SECONDS)))
      c.withFileSystemBind(dataDirPath, "/home/hadoop/data/")
      c.withCreateContainerCmdModifier(new Consumer[CreateContainerCmd]() {
        def accept(cmd: CreateContainerCmd): Unit = {
          cmd.withName("hadoop3")
        }
      })
    }

    lazy val byzerLangContainer = new ByzerLangContainer(clusterName).configure { c =>
      c.withNetworkAliases(ByzerCluster.appendClusterName(networkAliases))
      c.withNetwork(network)
      c.addExposedPorts(9003, 4040, 8265, 10002)
      c.setWaitStrategy(new HttpWaitStrategy()
        .forPort(9003).forStatusCode(200)
        .withStartupTimeout(Duration.of(3000, SECONDS)))
      c.withFileSystemBind(DockerUtils.getLibPath + DockerUtils.getJarName,
        "/home/deploy/byzer-lang/main/" + DockerUtils.getJarName, BindMode.READ_WRITE)
      c.dependsOn(hadoopContainer)
      c.withStartupAttempts(3)
      c.withCreateContainerCmdModifier(new Consumer[CreateContainerCmd]() {
        def accept(cmd: CreateContainerCmd): Unit = {
          cmd.withName(clusterName + (Random.nextInt & Integer.MAX_VALUE))
        }
      })

    }

    new ByzerCluster(byzerLangContainer, hadoopContainer)
  }

  def beforeAll(): Unit = {
    val rootPath: String = DockerUtils.getRootPath
    val packageFile: String = rootPath + DockerUtils.getFinalName + ".tar.gz"
    val libPath: String = DockerUtils.getLibPath
    val finalPath: String = libPath + DockerUtils.getJarName
    logInfo("The Byzer final jar path:" + finalPath)
    s"tar -xvf $packageFile -C $rootPath".!
    if (!new File(finalPath).exists) {
      throw new RuntimeException("Please make sure byzer-lang final jar is exists!")
    }
  }

  private def appendClusterName(name: String) = ByzerCluster.clusterName + "-" + name
}

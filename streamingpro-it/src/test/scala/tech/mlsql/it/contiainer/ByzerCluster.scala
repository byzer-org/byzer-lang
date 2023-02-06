package tech.mlsql.it.contiainer

import com.github.dockerjava.api.command.CreateContainerCmd
import org.apache.commons.lang3.StringUtils
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.Network.NetworkImpl
import org.testcontainers.containers.wait.strategy.{HostPortWaitStrategy, HttpWaitStrategy, Wait}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.it.contiainer.MySQLContainer.{DEFAULT_MYSQL_CONTAINER_PORT, DEFAULT_MYSQL_PORT}
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
class ByzerCluster(val byzerLangContainer: ByzerLangContainer, val hadoopContainer: HadoopContainer,
                   val mySQLContainer: MySQLContainer) extends Logging {

  def start(): Unit = {
    hadoopContainer.start()
    logInfo("Successfully started local hadoop container.")
    byzerLangContainer.start()
    logInfo("Successfully started local byzer-lang container.")
    mySQLContainer.start()
    logInfo("Successfully started local mysql container.")
  }

  def stop(): Unit = {
    byzerLangContainer.stop()
    logInfo("Successfully stop local byzer-lang container.")
    if (byzerLangContainer.container != null && byzerLangContainer.container.getContainerId != null) {
      Thread.sleep(10000)
    }
    hadoopContainer.stop()
    logInfo("Successfully stop local hadoop container.")
    mySQLContainer.stop()
    logInfo("Successfully stop mysql container.")
  }

}

object ByzerCluster extends Logging {
  lazy private val network = NetworkImpl.builder.build
  private val clusterName = "byzer-it"
  private val networkAliases = "byzer-network"

  def forSpec(dataDirPath: String): ByzerCluster = {
    beforeAll()
    lazy val hadoopContainer: HadoopContainer = new HadoopContainer(clusterName).configure { c =>
      c.addExposedPorts(9870, 8088, 19888, 10002, 8042, 8020, 9866, 9001)
      c.withNetwork(network)
      c.withNetworkAliases(ByzerCluster.appendClusterName(networkAliases))
      c.setWaitStrategy(new HttpWaitStrategy()
        .forPort(8088).forPath("/cluster").forStatusCode(200)
        .withStartupTimeout(Duration.of(1500, SECONDS)))
      if (StringUtils.isNoneBlank(dataDirPath)) {
        c.withFileSystemBind(dataDirPath, "/home/hadoop/data/")
      } else {
        logWarning("The data directory is empty, failed to initialize data!")
      }
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
      c.withFileSystemBind(DockerUtils.getCurProjectRootPath + "src/test/resources/byzer.properties",
        "/home/deploy/byzer-lang/conf/byzer.properties", BindMode.READ_WRITE)
      c.withFileSystemBind(DockerUtils.getCurProjectRootPath + "src/test/resources/byzer.properties.override",
        "/home/deploy/byzer-lang/conf/byzer.properties.override", BindMode.READ_WRITE)
      c.dependsOn(hadoopContainer)
      c.withStartupAttempts(3)
      val memoryInBytes = 16 * 1024 * 1024 * 1024
      val memorySwapInBytes = 16 * 1024 * 1024 * 1024 * 2
      c.withCreateContainerCmdModifier(new Consumer[CreateContainerCmd]() {
        def accept(cmd: CreateContainerCmd): Unit = {
          cmd.withMemory(memoryInBytes)
          cmd.withMemorySwap(memorySwapInBytes)
          cmd.withName(clusterName + (Random.nextInt & Integer.MAX_VALUE))
        }
      })
    }

    lazy val mySQLContainer = new MySQLContainer("mysql:5.7",
      exposedHostPort = DEFAULT_MYSQL_PORT,
      exposedContainerPort = DEFAULT_MYSQL_CONTAINER_PORT,
      clusterName = clusterName,
    ).configure { c =>
      c.withNetworkAliases(ByzerCluster.appendClusterName(networkAliases))
      c.withNetwork(network)
      c.addExposedPorts(3306)
      c.addEnv("MYSQL_ROOT_PASSWORD", "root")
      c.addEnv("MYSQL_ROOT_HOST", "%")
      c.setWaitStrategy(new HostPortWaitStrategy()
        .withStartupTimeout(Duration.of(3000, SECONDS)))
      c.withCreateContainerCmdModifier(new Consumer[CreateContainerCmd]() {
        def accept(cmd: CreateContainerCmd): Unit = {
          cmd.withName(clusterName + "-mysql")
        }
      })
    }

    new ByzerCluster(byzerLangContainer, hadoopContainer, mySQLContainer)
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

  private def appendClusterName(name: String): String = ByzerCluster.clusterName + "-" + name
}

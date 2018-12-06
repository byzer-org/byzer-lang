package tech.mlsql.cluster.service.elastic_resource.local

import net.sf.json.JSONObject
import streaming.common.shell.ShellCommand
import streaming.log.Logging
import tech.mlsql.cluster.model.{Backend, EcsResourcePool}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
object LocalDeployInstance extends Logging {
  def deploy(id: Int): Boolean = {
    val ecs = EcsResourcePool.find(id)
    var success = true
    try {
      logInfo(
        s"""
           |${ecs.getLoginUser} login in ${ecs.getIp} and execute command with user ${ecs.getExecuteUser}.
           |The content of script:
           |${script(ecs)}
         """.stripMargin)
      ShellCommand.sshExec(ecs.getKeyPath, ecs.getIp, ecs.getLoginUser, script(ecs), ecs.getExecuteUser)
    } catch {
      case e: Exception =>
        logError("fail to deploy new instance", e)
        success = false
    }
    if (success) {
      logInfo(s"Command execute successful, remove ${ecs.getIp} with name ${ecs.getName} from EcsResourcePool to Backend table.")
      Backend.newBackend(Map(
        "url" -> (ecs.getIp + ":" + getPort(ecs)),
        "tag" -> ecs.getTag,
        "name" -> ecs.getName
      ).asJava).save()
      ecs.delete()
    }
    return success

  }

  def getPort(ecs: EcsResourcePool) = {
    val port = JSONObject.fromObject(ecs.getMlsqlConfig).asScala.filter { k =>
      k._1.toString == "streaming.driver.port"
    }.map(f => f._2.toString).headOption
    require(port.isDefined, "MLSQL port should be configured.")
    port.get
  }

  private def script(ecs: EcsResourcePool) = {
    val configBuffer = ArrayBuffer[String]()
    JSONObject.fromObject(ecs.getMlsqlConfig).asScala.map { k =>
      val key = k._1.toString
      val value = k._2.toString
      if (key.startsWith("streaming.")) {
        configBuffer += s"-${key} $value"
      } else {
        configBuffer += s"--${key} $value"
      }
    }
    val startUpConfig = configBuffer.mkString("\\\n")
    s"""
       |#bin/bash
       |export SPARK_HOME=${ecs.getSparkHome}
       |cd $$SPARK_HOME
       |nohup ./bin/spark-submit --class streaming.core.StreamingApp \
       |        ${startUpConfig} > wow.log &
     """.stripMargin
  }

}

package tech.mlsql.cluster.service.elastic_resource.local

import net.sf.json.JSONObject
import streaming.common.shell.ShellCommand
import tech.mlsql.cluster.model.{Backend, EcsResourcePool}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */
object DeployInstance {
  def deploy(id: Int) = {
    val ecs = EcsResourcePool.find(id)
    ShellCommand.sshExec(ecs.getKeyPath, ecs.getIp, ecs.getLoginUser, script(ecs), ecs.getExecuteUser)

    Backend.newBackend(Map(
      "url" -> (ecs.getIp + ":" + getPort(ecs)),
      "tag" -> ecs.getTag,
      "name" -> ecs.getName
    ).asJava).save()
  }

  private def getPort(ecs: EcsResourcePool) = {
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

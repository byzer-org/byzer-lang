package tech.mlsql.cluster.commands

import tech.mlsql.cluster.model.{EcsResourcePool, ElasticMonitor}
import tech.mlsql.cluster.service.elastic_resource.{JobNumAwareAllocateStrategy, LocalResourceAllocation}

import scala.collection.JavaConverters._

/**
  * 2018-12-09 WilliamZhu(allwefantasy@gmail.com)
  */
object Command {
  def deploy = {
    val allocate = new JobNumAwareAllocateStrategy()
    val createECS = EcsResourcePool.newOne(Map(
      "ip" -> "47.97.167.88",
      "keyPath" -> "~/.ssh/mlsql-build-env-local",
      "loginUser" -> "root",
      "name" -> "backend2",
      "sparkHome" -> "/home/webuser/apps/spark-2.3",
      "mlsqlHome" -> "/home/webuser/apps/mlsql",
      "mlsqlConfig" ->
        """
          |{"master":"local",
          |"name":"mlsql",
          |"conf":"spark.serializer=org.apache.spark.serializer.KryoSerializer",
          |"streaming.name":"mlsql",
          |"streaming.driver.port":"9003",
          |"streaming.spark.service":"true",
          |"streaming.platform":"spark",
          |"streaming.rest":"true"
          |}
        """.stripMargin,
      "executeUser" -> "webuser",
      "tag" -> "jack"
    ).asJava, true)

    val monitor = ElasticMonitor.newOne(Map(
      "name" -> "jack-monitor",
      "tag" -> "jack",
      "minInstances" -> "1",
      "maxInstances" -> "3",
      "allocateType" -> "local",
      "allocateStrategy" -> "JobNumAwareAllocateStrategy"
    ).asJava)

    allocate.allocate(LocalResourceAllocation("jack"), monitor)
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.mlsql.cluster.test

import java.util

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Bootstrap
import org.scalatest.{FlatSpec, Matchers}
import tech.mlsql.cluster.model.{Backend, EcsResourcePool, ElasticMonitor}
import tech.mlsql.cluster.service.elastic_resource.local.LocalDeployInstance
import tech.mlsql.cluster.service.elastic_resource.{BaseResource, JobNumAwareAllocateStrategy, LocalResourceAllocation, LocalResourceDeAllocation}

import scala.collection.JavaConverters._

/**
  * 2018-12-07 WilliamZhu(allwefantasy@gmail.com)
  */
class DeploySpec extends FlatSpec with Matchers {

  def mockServer = {
    try {
      if (ServiceFramwork.injector == null) {
        ServiceFramwork.disableHTTP()
        ServiceFramwork.enableNoThreadJoin()
        Bootstrap.main(Array())
      }
    } catch {
      case e: Exception =>
    }
  }

  val createECS = () => EcsResourcePool.newOne(Map(
    "ip" -> "127.0.0.1",
    "keyPath" -> "keyPath",
    "loginUser" -> "root",
    "name" -> "backend2",
    "sparkHome" -> "/home/spark",
    "mlsqlHome" -> "/home/mlsql",
    "mlsqlConfig" ->
      """
        |{"master":"local",
        |"name":"mlsql",
        |"conf":"spark.serializer=org.apache.spark.serializer.KryoSerializer",
        |"streaming.name":"mlsql",
        |"streaming.driver.port":"9003",
        |"streaming.spark.service":"true",
        |"streaming.platform":"spark"
        |}
      """.stripMargin,
    "executeUser" -> "webuser",
    "tag" -> "jack"
  ).asJava, false)

  "deploy mlsql instance" should "should work in local mode" in {
    mockServer
    val ecs = createECS()
    val success = LocalDeployInstance._deploy(ecs, dryRun = true)
    assume(success)
  }

  "JobNumAwareAllocateStrategy" should "allocate new instance when busy" in {
    mockServer
    val monitor = ElasticMonitor.newOne(Map(
      "name" -> "jack-monitor",
      "tag" -> "jack",
      "minInstances" -> "1",
      "maxInstances" -> "3",
      "allocateType" -> "local",
      "allocateStrategy" -> "JobNumAwareAllocateStrategy"
    ).asJava,false)

    val backend = Backend.newOne(Map(
      "url" -> "127.0.0.1:9003",
      "tag" -> "jack",
      "name" -> "backend1"
    ).asJava, false)
    val allocate = new JobNumAwareAllocateStrategy() {
      override def fetchAvailableResources: util.List[EcsResourcePool] = {
        val ecs = createECS()
        List(ecs).asJava
      }

      override def fetchBackendsWithTags(tagsStr: String): Set[Backend] = {
        Set(backend)
      }

      override def fetchAllActiveBackends(): Set[Backend] = {
        // here we will make all backends busy
        Set(backend)
      }

      override def dryRun(): Boolean = true
    }
    var planRes: Option[BaseResource] = None
    (0 until 30).foreach { time =>
      planRes = allocate.plan(monitor.getTag.split(",").toSeq, monitor)
      assume(!planRes.isDefined)
    }

    planRes = allocate.plan(monitor.getTag.split(",").toSeq, monitor)
    assume(planRes.isDefined)

    print(planRes)

    planRes.get match {
      case LocalResourceAllocation(tags) => assume(tags == "jack")
    }

    val res = allocate.allocate(planRes.get, monitor)
    assume(res)

  }

  "JobNumAwareAllocateStrategy" should "remove new instance when idle" in {
    mockServer
    val monitor = ElasticMonitor.newOne(Map(
      "name" -> "jack-monitor",
      "tag" -> "jack",
      "minInstances" -> "1",
      "maxInstances" -> "3",
      "allocateType" -> "local",
      "allocateStrategy" -> "JobNumAwareAllocateStrategy"
    ).asJava,false)

    val backend = Backend.newOne(Map(
      "url" -> "127.0.0.1:9003",
      "tag" -> "jack",
      "name" -> "backend1"
    ).asJava, false)

    val backend2 = Backend.newOne(Map(
      "url" -> "127.0.0.1:9003",
      "tag" -> "jack",
      "name" -> "backend2"
    ).asJava, false)


    val allocate = new JobNumAwareAllocateStrategy() {
      override def fetchAvailableResources: util.List[EcsResourcePool] = {
        val ecs = createECS()
        List(ecs).asJava
      }

      override def fetchBackendsWithTags(tagsStr: String): Set[Backend] = {
        Set(backend, backend2)
      }

      override def fetchAllActiveBackends(): Set[Backend] = {
        // here we will make all backends busy
        Set(backend)
      }

      override def dryRun(): Boolean = true

      override def ecsResourcePoolForTesting(backend: Backend): EcsResourcePool = {
        createECS()
      }

      override def fetchAllNonActiveBackendsWithTags(tagsStr: String): Set[Backend] = {
        Set(backend2)
      }
    }
    var planRes: Option[BaseResource] = None
    (0 until 30).foreach { time =>
      planRes = allocate.plan(monitor.getTag.split(",").toSeq, monitor)
      assume(!planRes.isDefined)
    }

    planRes = allocate.plan(monitor.getTag.split(",").toSeq, monitor)
    assume(planRes.isDefined)

    print(planRes)

    planRes.get match {
      case LocalResourceDeAllocation(tags) => assume(tags == "jack")
    }

    val res = allocate.allocate(planRes.get, monitor)
    assume(res)

  }
  

}

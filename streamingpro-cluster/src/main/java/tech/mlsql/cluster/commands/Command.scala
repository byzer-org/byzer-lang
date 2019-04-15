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
    ).asJava,true)

    allocate.allocate(LocalResourceAllocation("jack"), monitor)
  }
}

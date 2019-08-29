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

package streaming.test.rest

import net.csdn.common.collections.WowCollections
import net.csdn.junit.BaseControllerTest
import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import tech.mlsql.job.JobManager

/**
  * 2018-12-06 WilliamZhu(allwefantasy@gmail.com)
  */
//class RestAPISpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll {
//
//
//  "/run/script" should "work fine" in {
//    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
//      //执行sql
//      implicit val spark = runtime.sparkSession
//      mockServer
//
//      JobManager.init(spark)
//      val controller = new BaseControllerTest()
//
//      val response = controller.get("/run/script", WowCollections.map(
//        "sql", "select 1 as a as t;"
//      ));
//      assume(response.status() == 200)
//      assume(response.originContent() == "[{\"a\":1}]")
//      JobManager.shutdown
//    }
//  }
//
//  "/run/script auth" should "work fine" in {
//    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
//      //执行sql
//      implicit val spark = runtime.sparkSession
//      mockServer
//
//      JobManager.init(spark)
//      val controller = new BaseControllerTest()
//
//      val path = this.getClass().getClassLoader().getResource("").getPath()
//        .replace("test-" ,"")
//
//      val response = controller.get("/run/script", WowCollections.map(
//        "sql", s"include hdfs.`${path}/test/include-set.txt` ;select '$${xx}' as a as t;"
//        ,"skipAuth" ,"false"
//        ,"owner" ,"latincross"
//      ));
//      assume(response.status() == 200)
//      assume(response.originContent() == "[{\"a\":\"latincross\"}]")
//      JobManager.shutdown
//    }
//  }
//}

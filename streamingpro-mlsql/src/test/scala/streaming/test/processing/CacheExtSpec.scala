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

package streaming.test.processing

import org.apache.spark.execution.TestHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec

class CacheExtSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "cache" should "cache or uncache successfully" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |select "a" as a as table1;
          |run table1 as CacheExt.`` where execute="cache";
        """.stripMargin, sq)

      val df = spark.table("table1")
      assume(spark.sharedState.cacheManager.lookupCachedData(df).isDefined)

      ScriptSQLExec.parse(
        """
          |run table1 as CacheExt.`` where execute="uncache";
        """.stripMargin, sq)

      assume(!spark.sharedState.cacheManager.lookupCachedData(df).isDefined)
    }
  }

  "cache" should "supports eager" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |select "a" as a as table1;
          |run table1 as CacheExt.`` where execute="cache" ;
        """.stripMargin, sq)

      var df = spark.table("table1")
      assume(!isCacheBuild(df))

      ScriptSQLExec.parse(
        """
          |select "a" as a as table1;
          |run table1 as CacheExt.`` where execute="cache" and isEager="true";
        """.stripMargin, sq)

      df = spark.table("table1")
      assume(isCacheBuild(df))

    }
  }

  def isCacheBuild(df: DataFrame)(implicit spark: SparkSession) = {
    TestHelper.isCacheBuild(df)(spark)
  }

}

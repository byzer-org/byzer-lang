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

package streaming.core

import net.sf.json.JSONObject
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime

/**
  * Created by allwefantasy on 26/4/2018.
  */
class UDFSpec extends BasicSparkOperation with SpecFunctions {
  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )


  "keepChinese" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val query = "你◣◢︼【】┅┇☽☾✚〓▂▃▄▅▆▇█▉▊▋▌▍▎▏↔↕☽☾の·▸◂▴▾┈┊好◣◢︼【】┅┇☽☾✚〓▂▃▄▅▆▇█▉▊▋▌▍▎▏↔↕☽☾の·▸◂▴▾┈┊啊，..。，！?katty"

      var res = spark.sql(s"""select keepChinese("${query}",false,array()) as jack""").toJSON.collect()
      var keyRes = JSONObject.fromObject(res(0)).getString("jack")
      assume(keyRes == "你好啊")

      res = spark.sql(s"""select keepChinese("${query}",true,array()) as jack""").toJSON.collect()
      keyRes = JSONObject.fromObject(res(0)).getString("jack")
      assume(keyRes == "你好啊，..。，！?")

      res = spark.sql(s"""select keepChinese("${query}",false,array("。")) as jack""").toJSON.collect()
      keyRes = JSONObject.fromObject(res(0)).getString("jack")
      assume(keyRes == "你好啊。")

    }
  }


}

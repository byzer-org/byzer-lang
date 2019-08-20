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

import java.io.File

import net.sf.json.JSONObject
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, NotToRunTag, SpecFunctions}
import streaming.dsl.ScriptSQLExec

/**
  * Created by allwefantasy on 12/9/2018.
  */
class TextModuleSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  def createFile(path: String, content: String) = {
    val f = new File("/tmp/abc.txt")
    if (!f.exists()) {
      FileUtils.write(f, "天了噜", "utf-8")
    }
  }

  val isAnjsAnalyzerLoaded = () => {
    try {
      Class.forName("org.ansj.splitWord.analysis.NlpAnalysis")
      true
    } catch {
      case e: Exception =>
        false
    }
  }

  def analyzerContext[R](block: () => R): R = {
    if (isAnjsAnalyzerLoaded()) {
      block()
    } else assume(1 == 1).asInstanceOf[R]
  }
  

  "extract with dic" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      analyzerContext(() => {
        //执行sql
        implicit val spark = runtime.sparkSession
        //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
        var sq = createSSEL
        sq = createSSEL
        ScriptSQLExec.parse(loadSQLScriptStr("token-extract"), sq)
        val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
        println(res)
        import scala.collection.JavaConversions._
        assume(JSONObject.fromObject(res).getJSONArray("keywords").
          filter(f => f.asInstanceOf[String].
            startsWith("天了噜")).size > 0)
      })

    }
  }
}

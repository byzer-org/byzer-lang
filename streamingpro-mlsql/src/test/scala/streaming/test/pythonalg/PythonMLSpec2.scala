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

package streaming.test.pythonalg

import java.io.File
import java.nio.charset.Charset
import java.util.UUID

import com.google.common.io.Files
import net.csdn.common.collections.WowCollections
import net.csdn.junit.BaseControllerTest
import net.sf.json.JSONArray
import org.apache.spark.SparkCoreVersion
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge
import streaming.test.pythonalg.code.ScriptCode
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros._
import tech.mlsql.common.utils.shell.ShellCommand
import tech.mlsql.job.JobManager

import scala.io.Source

/**
  * Created by allwefantasy on 26/5/2018.
  */
class PythonMLSpec2 extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  copySampleLibsvmData

  def getHome = {
    getClass.getResource("").getPath.split("streamingpro\\-mlsql").head
  }

  def getExampleProject(name: String) = {
    //sklearn_elasticnet_wine
    getHome + "examples/" + name
  }

  def getPysparkVersion = {
    val version = SparkCoreVersion.exactVersion
    if (version == "2.2.0") "2.2.1"
    else version
  }


  "SQLPythonAlgTrain" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithAPI, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      mockServer
      ShellCommand.exec("rm -rf /tmp/william/tmp/jack2")
      //SPARK_VERSION
      val sq = createSSEL(spark, "")
      val projectName = "sklearn_elasticnet_wine"
      var projectPath = getExampleProject(projectName)

      var newpath = s"/tmp/${UUID.randomUUID().toString}"
      ShellCommand.execCmd(s"cp -r ${projectPath} $newpath")

      val newcondafile = TemplateMerge.merge(Source.fromFile(new File(newpath + "/conda.yaml")).getLines().mkString("\n"),
        Map("SPARK_VERSION" -> getPysparkVersion))
      Files.write(newcondafile, new File(newpath + "/conda.yaml"), Charset.forName("utf-8"))

      projectPath = newpath

      val scriptCode = ScriptCode(s"/tmp/${projectName}", projectPath)

      val config = Map(
        str[ScriptCode](_.featureTablePath) -> scriptCode.featureTablePath,
        str[ScriptCode](_.modelPath) -> scriptCode.modelPath,
        str[ScriptCode](_.projectPath) -> scriptCode.projectPath,
        "kv" -> ""
      )

      //train
      ScriptSQLExec.parse(TemplateMerge.merge(ScriptCode.train, config), sq)

      var table = sq.getLastSelectTable().get
      val status = spark.sql(s"select * from ${table}").collect().map(f => f.getAs[String]("status")).head
      assert(status == "success")

      //batch predict

      ScriptSQLExec.parse(TemplateMerge.merge(ScriptCode.batchPredict, config), sq)
      table = sq.getLastSelectTable().get
      val rowsNum = spark.sql(s"select * from ${table}").collect()
      assert(rowsNum.size > 0)

      assert(new File(s"/tmp/${projectName}/tmp/output/").listFiles().toList.filter { f =>
        f.getName.endsWith("-0.json")
      }.size > 0)

      ScriptSQLExec.parse(TemplateMerge.merge(ScriptCode.apiPredict, config), sq)

      // api predict
      def request = {
        JobManager.init(spark)
        val controller = new BaseControllerTest()

        val response = controller.get("/model/predict", WowCollections.map(
          "sql", "select pj(vec_dense(features)) as p1 ",
          "data",
          s"""[{"features":[ 0.045, 8.8, 1.001, 45.0, 7.0, 170.0, 0.27, 0.45, 0.36, 3.0, 20.7 ]}]""",
          "dataType", "row"
        ));
        assume(response.status() == 200)
        JobManager.shutdown
        JSONArray.fromObject(response.originContent())
      }

      assert(request.size() > 0)
    }
  }

  "SQLPythonAlgTrain keepLocalDirectory" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      mockServer
      ShellCommand.exec("rm -rf /tmp/william/tmp/jack2")
      val sq = createSSEL(spark, "")
      val projectName = "sklearn_elasticnet_wine"
      var projectPath = getExampleProject(projectName)

      var newpath = s"/tmp/${UUID.randomUUID().toString}"
      ShellCommand.execCmd(s"cp -r ${projectPath} $newpath")

      val newcondafile = TemplateMerge.merge(Source.fromFile(new File(newpath + "/conda.yaml")).getLines().mkString("\n"),
        Map("SPARK_VERSION" -> getPysparkVersion))
      Files.write(newcondafile, new File(newpath + "/conda.yaml"), Charset.forName("utf-8"))

      projectPath = newpath

      val scriptCode = ScriptCode(s"/tmp/${projectName}", projectPath)

      val config = Map(
        str[ScriptCode](_.featureTablePath) -> scriptCode.featureTablePath,
        str[ScriptCode](_.modelPath) -> scriptCode.modelPath,
        str[ScriptCode](_.projectPath) -> scriptCode.projectPath,
        "kv" -> """ and keepLocalDirectory="true" """
      )

      //train
      ScriptSQLExec.parse(TemplateMerge.merge(ScriptCode.train, config), sq)

      var table = sq.getLastSelectTable().get
      val status = spark.sql(s"select * from ${table}").collect().map(f => f.getAs[String]("status")).head
      assert(status == "success")


    }
  }

  "SQLPythonAlg auto create project" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      mockServer
      ShellCommand.exec("rm -rf /tmp/william/tmp/jack2")
      var sq = createSSEL(spark, "")
      //train
      ScriptSQLExec.parse(ScriptCode._j2, sq)

      var table = sq.getLastSelectTable().get
      val res = spark.sql(s"select * from output").collect()
      assert(res.length == 1)
      assert(res.head.getAs[String](0).contains("jack"))

      //      sq = createSSEL(spark, "")
      //      ScriptSQLExec.parse(ScriptCode._j2_PREDICT, sq)
    }
  }

  "SQLPythonParallelExt " should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      mockServer
      ShellCommand.exec("rm -rf /tmp/william/tmp/jack2")
      val sq = createSSEL(spark, "")
      //train
      ScriptSQLExec.parse(ScriptCode._j1, sq)

      var table = sq.getLastSelectTable().get
      val res = spark.sql(s"select * from output").collect()
      assert(res.length == 1)
      assert(res.head.getAs[String](0).contains("jack"))

    }
  }

  "SQLPythonParallelExt with paritition" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      mockServer
      ShellCommand.exec("rm -rf /tmp/william/tmp/jack2")
      ShellCommand.exec("mkdir -p /tmp/resource")
      ShellCommand.exec("touch /tmp/resource/a.txt")
      val sq = createSSEL(spark, "")
      //train
      ScriptSQLExec.parse(ScriptCode._j3, sq)

      var table = sq.getLastSelectTable().get
      spark.sql(s"select * from output").show()
      val res = spark.sql(s"select * from output").collect()
      assert(res.head.getAs[String]("wow").contains("jack"))
      assert(res.head.getAs[Long]("resource_a") == 2)
      assert(res.head.getAs[Long]("resource_b") == 2)


    }
  }

}

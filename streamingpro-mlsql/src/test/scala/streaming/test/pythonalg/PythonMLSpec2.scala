package streaming.test.pythonalg

import java.io.File
import java.nio.charset.Charset
import java.util.UUID

import com.google.common.io.Files
import net.sf.json.JSONArray
import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.SparkCoreVersion
import org.apache.spark.streaming.BasicSparkOperation
import streaming.common.ScalaMethodMacros._
import streaming.common.shell.ShellCommand
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge
import streaming.test.pythonalg.code.ScriptCode

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

      ScriptSQLExec.parse(TemplateMerge.merge(ScriptCode.apiPredict, config), sq)

      // api predict
      def request = {
        val sql = "select pj(vec_dense(features)) as p1 "

        val res = Request.Post("http://127.0.0.1:9003/model/predict").bodyForm(Form.form().
          add("sql", sql).
          add("data", s"""[{"features":[ 0.045, 8.8, 1.001, 45.0, 7.0, 170.0, 0.27, 0.45, 0.36, 3.0, 20.7 ]}]""").
          add("dataType", "row")
          .build()).execute().returnContent().asString()
        JSONArray.fromObject(res)
      }

      assert(request.size() > 0)
    }
  }

  "SQLPythonAlgTrain keepLocalDirectory" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
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

}

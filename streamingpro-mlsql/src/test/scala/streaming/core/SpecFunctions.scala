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

import java.io.{File, FileNotFoundException}
import java.sql.{DriverManager, Statement}

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Bootstrap
import org.apache.commons.io.FileUtils
import org.apache.http.HttpVersion
import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.job.{JobManager, MLSQLJobInfo, MLSQLJobProgress, MLSQLJobType}

/**
  * Created by allwefantasy on 28/4/2018.
  */
trait SpecFunctions {

  def password = "mlsql"

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

  def request(url: String, params: Map[String, String]) = {
    val form = Form.form()
    params.map(f => form.add(f._1, f._2))
    val res = Request.Post(url)
      .useExpectContinue()
      .version(HttpVersion.HTTP_1_1).bodyForm(form.build())
      .execute().returnResponse()
    if (res.getStatusLine.getStatusCode != 200) {
      null
    } else {
      new String(EntityUtils.toByteArray(res.getEntity))
    }
  }

  def createSSEL(implicit spark: SparkSession, defaultPathPrefix: String = "/tmp/william", groupId: String = "-") = {
    JobManager.initForTest(spark)
    JobManager.addJobManually(MLSQLJobInfo(
      "william", MLSQLJobType.SCRIPT, "", "", groupId, new MLSQLJobProgress(), System.currentTimeMillis(), -1
    ))
    val context = new ScriptSQLExecListener(spark, defaultPathPrefix, Map())
    context.addEnv("HOME", context.pathPrefix(None))
    ScriptSQLExec.setContext(new MLSQLExecuteContext(context, "william", "/tmp/william", groupId, Map()))
    context
  }

  def createSSELWithJob(spark: SparkSession, jobName: String, groupId: String) = {
    JobManager.initForTest(spark)
    JobManager.addJobManually(MLSQLJobInfo(
      "william", MLSQLJobType.SCRIPT, "", "", groupId, new MLSQLJobProgress(), System.currentTimeMillis(), -1
    ))
    val context = new ScriptSQLExecListener(spark, "/tmp/william", Map())
    context.addEnv("HOME", context.pathPrefix(None))
    ScriptSQLExec.setContext(new MLSQLExecuteContext(context, "william", "/tmp/william", groupId, Map()))
    context
  }

  def addStreamJob(spark: SparkSession, jobName: String, grouId: String) = {
    JobManager.initForTest(spark)

    JobManager.addJobManually(MLSQLJobInfo(
      owner = "william", jobType = MLSQLJobType.STREAM, jobName = jobName, "", groupId = grouId, new MLSQLJobProgress(), -1, -1
    ))
  }

  def addBatchJob(spark: SparkSession, jobName: String, grouId: String) = {
    JobManager.initForTest(spark)

    JobManager.addJobManually(MLSQLJobInfo(
      owner = "william", jobType = MLSQLJobType.SCRIPT, jobName = jobName, "", groupId = grouId, new MLSQLJobProgress(), -1, -1
    ))
  }

  def dropTables(tables: Seq[String])(implicit spark: SparkSession) = {
    try {
      tables.foreach { table =>
        spark.sql("drop table " + table).count()
      }
    } catch {
      case e: Exception =>
    }
  }

  def jdbc(ddlStr: String, connectStat: String) = {
    Class.forName("com.mysql.jdbc.Driver")
    val con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
      "root",
      password)
    var stat: Statement = null
    try {
      stat = con.createStatement()
      stat.execute(ddlStr)

    } finally {
      if (stat != null) {
        stat.close()
      }
      if (con != null) {
        con.close()
      }

    }


  }

  def loadSQLScriptStr(name: String) = {
    val file = s"/test/sql/${name}.sql"
    val stream = SpecFunctions.this.getClass.getResourceAsStream(file)
    if (stream == null) throw new RuntimeException(s"load file: ${file} failed,please chech the path")
    scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
  }

  def loadSQLStr(name: String) = {
    scala.io.Source.fromInputStream(SpecFunctions.this.getClass.getResourceAsStream(s"/test/sql/${name}")).getLines().mkString("\n")
  }

  def loadDataStr(name: String) = {
    scala.io.Source.fromInputStream(SpecFunctions.this.getClass.getResourceAsStream(s"/data/mllib/${name}")).getLines().mkString("\n")
  }

  def loadPythonStr(name: String) = {
    scala.io.Source.fromInputStream(SpecFunctions.this.getClass.getResourceAsStream(s"/python/${name}")).getLines().mkString("\n")
  }

  def getDirFromPath(filePath: String) = {
    filePath.stripSuffix("/").split("/").dropRight(1).mkString("/")
  }

  def delDir(file: String) = {
    try {
      require(file.stripSuffix("/").split("/").size > 1, s"delete $file  maybe too dangerous")
      FileUtils.forceDelete(new File(file))
    }
    catch {
      case ex: FileNotFoundException => println(ex)
    }
  }

  def writeStringToFile(file: String, content: String) = {
    FileUtils.forceMkdir(new File(getDirFromPath(file)))
    FileUtils.writeStringToFile(new File(file), content)
  }

  def writeByteArrayToFile(file: String, content: Array[Byte]) = {
    FileUtils.forceMkdir(new File(getDirFromPath(file)))
    FileUtils.writeByteArrayToFile(new File(file), content)
  }

  def copySampleLibsvmData = {
    writeStringToFile("/tmp/william/sample_libsvm_data.txt", loadDataStr("sample_libsvm_data.txt"))
    writeStringToFile("/tmp/william/sample_lda_libsvm_data.txt", loadDataStr("sample_lda_libsvm_data.txt"))
  }

  def copySampleMovielensRratingsData = {
    writeStringToFile("/tmp/william/sample_movielens_ratings.txt", loadDataStr("als/sample_movielens_ratings.txt"))
  }

  def copyTitanic = {
    writeStringToFile("/tmp/william/titanic.csv", loadDataStr("titanic.csv"))
  }
}

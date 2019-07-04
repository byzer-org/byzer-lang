//package tech.mlsql.test.client
//
//import java.nio.charset.Charset
//import java.util.UUID
//
//import net.sf.json.{JSONArray, JSONObject}
//import org.apache.http.client.fluent.{Form, Request}
//import org.apache.spark.streaming.BasicSparkOperation
//import org.scalatest.BeforeAndAfterAll
//import streaming.core.{BasicMLSQLConfig, SpecFunctions}
//
///**
//  * 2019-04-08 WilliamZhu(allwefantasy@gmail.com)
//  */
//class ClientModeSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll {
//
//  def server = {
//    "http://127.0.0.1:9003/"
//  }
//
//  def runScript(mlsql: String, options: Map[String, String] = Map[String, String]()) = {
//    val finalOptions = Map(
//      "sessionPerUser" -> "true",
//      "owner" -> "william",
//      "jobName" -> UUID.randomUUID().toString,
//      "show_stack" -> "true",
//      "skipAuth" -> "true",
//      "sql" -> mlsql
//    ) ++ (options - "endpoint")
//
//    val endpoint = options.getOrElse("endpoint", "/run/script")
//
//
//    val formParams = Form.form()
//    finalOptions.foreach { tuple =>
//      formParams.add(tuple._1, tuple._2)
//    }
//
//    val res = Request.Post(s"${server}/${endpoint}").connectTimeout(60 * 60 * 1000)
//      .socketTimeout(60 * 60 * 1000).bodyForm(formParams.build())
//      .execute().returnContent().asString(Charset.forName("utf-8"))
//    res
//  }
//
//
//  "Rest API" should "show/kill jobs" in {
//    val jobName = "test1"
//
//    def runSyncJob(jobName: String) = {
//      new Thread(new Runnable {
//        override def run(): Unit = {
//          runScript(
//            """
//              |select sleep(60000) as a as b;
//            """.stripMargin, Map("jobName" -> jobName))
//        }
//      }).start()
//    }
//
//    runSyncJob(jobName)
//    Thread.sleep(3000)
//
//    def getJobs = {
//      Request.Get(s"${server}/runningjobs").execute().returnContent().asString(Charset.forName("utf-8"))
//    }
//
//    var res = getJobs
//    JSONObject.fromObject(res).size() should be(1)
//
//    val killJobRes = runScript(s"run command as Kill.`${jobName}`;", Map("owner" -> "josh"))
//    assert(killJobRes.contains("You can not kill the job"))
//
//
//    Request.Post(s"${server}/killjob").bodyForm(Form.form().add("jobName", jobName).add("sessionPerUser", "true").build()).
//      execute().returnContent().asString(Charset.forName("utf-8"))
//
//    res = getJobs
//    JSONObject.fromObject(res).size() should be(0)
//
//    Thread.sleep(2000)
//
//
//    runSyncJob(jobName + "_")
//    Thread.sleep(2000)
//
//    res = getJobs
//    JSONObject.fromObject(res).size() should be(1)
//    runScript(s"run command as Kill.`${jobName + "_"}`;", Map("owner" -> "william"))
//    res = getJobs
//
//    JSONObject.fromObject(res).size() should be(0)
//
//  }
//
//  def waitJobDone(groupId: String, maxSize: Int = 60) = {
//    var finish = false
//    var count = 0
//    while (finish && count < maxSize) {
//      val res = runScript(s"load _mlsql_.`jobs/${groupId}` as output;")
//      finish = JSONArray.fromObject(res).size() == 0
//      count += 1
//      Thread.sleep(1000)
//    }
//
//  }
//
//  "python udf" should "work fine with null" in {
//    val res = runScript(
//      """
//        |set convert_data='''
//        |
//        |def apply(self,m):
//        |    return "data:" + str(m)
//        |''';
//        |
//        |load script.`convert_data` as scriptTable;
//        |
//        |register ScriptUDF.`scriptTable` as convert_data
//        |options lang="python" and dataType="string";
//        |
//        |select convert_data(null) as a ,"c" as c as b;
//      """.stripMargin)
//    res should include("data:None")
//  }
//}

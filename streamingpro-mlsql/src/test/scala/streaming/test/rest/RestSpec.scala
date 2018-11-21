package streaming.test.rest

import java.io.File

import net.sf.json.JSONArray
import org.apache.commons.io.FileUtils
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest._
import serviceframework.dispatcher.StrategyDispatcher
import streaming.common.ParamsUtil
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.core.{BasicMLSQLConfig, NotToRunTag, SpecFunctions}

/**
  * Created by allwefantasy on 25/4/2018.
  */
class RestSpec extends FlatSpec with Matchers with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterEach {

  val script_url = "http://127.0.0.1:9003/run/script"
  val sql_url = "http://127.0.0.1:9003/run/sql"
  val streaming_running_url = "http://127.0.0.1:9003/stream/jobs/running"
  val streaming_kill_url = "http://127.0.0.1:9003/stream/jobs/kill"
  val model_predict_url = "http://127.0.0.1:9003/model/predict"

  override def beforeEach() {

  }

  override def afterEach() {

  }


  def sql(sqlStr: String) = {
    request(sql_url, Map("sql" -> sqlStr))
  }

  def script(name: String) = {
    request(script_url, Map("sql" -> loadSQLScriptStr(name)))
  }


  "mllib-bayes" should "work" in {
    var res = script("mllib-bayes")
    assume(res == "{}")
    res = sql("select bayes_predict(features) from data as result limit 1")
    assume(res == s"""[{"UDF(features)":{"type":1,"values":[1.0,0.0]}}]""")
  }


  "streaming sql" should "work" in {
    sql("drop table if exists carbon_table2")
    var res = sql("CREATE TABLE carbon_table2 (\n      col1 STRING,\n      col2 STRING\n      )\n      STORED BY 'carbondata'\n      TBLPROPERTIES('streaming'='true')")
    assume(res == "[]")
    res = script("streaming")
    assume(res == "{}")
    Thread.sleep(3000)
    res = sql("select * from carbon_table2 limit 1")
    assume(JSONArray.fromObject(res).size() == 0)

    res = request(streaming_running_url, Map())
    assume(JSONArray.fromObject(res).size() == 1)
    val groupId = JSONArray.fromObject(res).getJSONObject(0).getString("groupId")
    res = request(streaming_kill_url, Map("groupId" -> groupId))
    res = request(streaming_running_url, Map())
    assume(JSONArray.fromObject(res).size() == 0)
  }


}

object RestSpec {
  var context: SparkRuntime = null
}

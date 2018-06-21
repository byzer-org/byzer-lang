package streaming.core

import net.sf.json.JSONArray
import org.apache.http.HttpVersion
import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by allwefantasy on 25/4/2018.
  */
class RestSpec extends FlatSpec with Matchers with SpecFunctions{

  val script_url = "http://127.0.0.1:9003/run/script"
  val sql_url = "http://127.0.0.1:9003/run/sql"
  val streaming_running_url = "http://127.0.0.1:9003/stream/jobs/running"
  val streaming_kill_url = "http://127.0.0.1:9003/stream/jobs/kill"
  val model_predict_url = "http://127.0.0.1:9003/model/predict"

  def sql(sqlStr: String) = {
    request(sql_url, Map("sql" -> sqlStr))
  }

  def script(name: String) = {
    request(script_url, Map("sql" -> loadSQLScriptStr(name)))
  }

  //make sure -Djava.library.path=[your-path]/jni was set
  "tensorflow training and predict" should "work" taggedAs (NotToRunTag) in {
    var res = request(script_url, Map("sql" -> loadSQLScriptStr("tensorflow")))
    assume(res == "{}")
    res = request(sql_url, Map("sql" -> "select vec_argmax(tf_predict(features,\"features\",\"label\",2)) as predict_label,\nlabel from newdata limit 1"))
    assume(res == s"""[{"predict_label":0,"label":{"type":0,"size":2,"indices":[0],"values":[1.0]}}]""")
  }

  "mysql connect" should "work" taggedAs (NotToRunTag) in {
    val res = request(script_url, Map("sql" -> loadSQLScriptStr("jdbc")))
    assume(res == "{}")
  }

  "mllib-bayes" should "work" taggedAs (NotToRunTag) in {
    var res = script("mllib-bayes")
    assume(res == "{}")
    res = sql("select bayes_predict(features) from data as result limit 1")
    assume(res == s"""[{"UDF(features)":{"type":1,"values":[1.0,0.0]}}]""")
  }

  "sk-bayes" should "work" taggedAs (NotToRunTag) in {
    var res = script("sk-bayes")
    assume(res == "{}")
    res = sql("select nb_predict(features) from data as result limit 1")
    assume(res == s"""[{"UDF(features)":{"type":1,"values":[0.0]}}]""")
  }

  "crawlersql" should "work" taggedAs (NotToRunTag) in {
    var res = script("crawlersql")
    assume(res == "{}")
    res = sql("select * from aritle_url_table_source limit 1")
    assume(JSONArray.fromObject(res).size() == 1)
  }

  "streaming sql" should "work" taggedAs (NotToRunTag) in {
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

  "sklearn model predict" should "work" taggedAs (NotToRunTag) in {
    val data = loadSQLStr("data.txt")
    var res = script("sk-bayes")
    assume(res == "{}")
    res = request(model_predict_url, Map("data" -> data, "sql" -> "select nb_predict(feature) as p"))
    assume(res == s"""{"p":{"type":1,"values":[0.0]}}""")
  }

}

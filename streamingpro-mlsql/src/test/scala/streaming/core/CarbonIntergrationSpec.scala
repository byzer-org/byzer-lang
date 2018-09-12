package streaming.core

import net.sf.json.JSONObject
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 12/9/2018.
  */
class CarbonIntergrationSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig{

  "script-support-drop" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val tableName = "visit_carbon5"

      var sq = createSSEL

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("mlsql-carbondata"), Map("tableName" -> tableName)), sq)
      Thread.sleep(1000)
      val res = spark.sql("select * from " + tableName).toJSON.collect()
      val keyRes = JSONObject.fromObject(res(0)).getString("a")
      assume(keyRes == "1")

      sq = createSSEL
      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("script-support-drop"), Map("tableName" -> tableName)), sq)
      try {
        spark.sql("select * from " + tableName).toJSON.collect()
        assume(0 == 1)
      } catch {
        case e: Exception =>
      }

    }
  }
  "carbondata save" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParamsWithCarbondata, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      var tableName = "visit_carbon3"

      dropTables(Seq(tableName))

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("mlsql-carbondata"), Map("tableName" -> tableName)), sq)
      Thread.sleep(1000)
      var res = spark.sql("select * from " + tableName).toJSON.collect()
      var keyRes = JSONObject.fromObject(res(0)).getString("a")
      assume(keyRes == "1")

      dropTables(Seq(tableName))

      sq = createSSEL
      tableName = "visit_carbon4"

      dropTables(Seq(tableName))

      ScriptSQLExec.parse(TemplateMerge.merge(loadSQLScriptStr("mlsql-carbondata-without-option"), Map("tableName" -> tableName)), sq)
      Thread.sleep(1000)
      res = spark.sql("select * from " + tableName).toJSON.collect()
      keyRes = JSONObject.fromObject(res(0)).getString("a")
      assume(keyRes == "1")
      dropTables(Seq(tableName))

    }
  }

}

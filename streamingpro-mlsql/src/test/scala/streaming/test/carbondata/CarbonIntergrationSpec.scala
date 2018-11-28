package streaming.test.carbondata

import net.sf.json.JSONObject
import org.apache.spark.{CarbonCoreVersion, SparkCoreVersion}
import org.apache.spark.streaming.BasicSparkOperation
import streaming.common.shell.ShellCommand
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, NotToRunTag, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 12/9/2018.
  */
class CarbonIntergrationSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  val checkCarbonDataCoreCompatibility = CarbonCoreVersion.coreCompatibility(SparkCoreVersion.version, SparkCoreVersion.exactVersion)

  "script-support-drop" should "work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithCarbondata, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      try {
        if (checkCarbonDataCoreCompatibility) {
          val tableName = "visit_carbon5"
          var sql =
            """
              |select "1" as a
              |as mtf1;
              |
              |save overwrite mtf1
              |as carbondata.`-`
              |options mode="overwrite"
              |and tableName="${tableName}"
              |and implClass="org.apache.spark.sql.CarbonSource";
            """.stripMargin

          var sq = createSSEL

          ScriptSQLExec.parse(TemplateMerge.merge(sql, Map("tableName" -> tableName)), sq)
          val res = spark.sql("select * from " + tableName).toJSON.collect()
          val keyRes = JSONObject.fromObject(res(0)).getString("a")
          assume(keyRes == "1")

          sql =
            """
              |drop table ${tableName};
            """.stripMargin

          sq = createSSEL
          ScriptSQLExec.parse(TemplateMerge.merge(sql, Map("tableName" -> tableName)), sq)
          try {
            spark.sql("select * from " + tableName).toJSON.collect()
            assume(0 == 1)
          } catch {
            case e: Exception =>
          }
        }
      } finally {
        ShellCommand.execCmd("rm -rf /tmp/carbondata/store")
        ShellCommand.execCmd("rm -rf /tmp/carbondata/meta")
      }
    }
  }
  "carbondata save" should "work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithCarbondata, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      if (checkCarbonDataCoreCompatibility) {
        try {
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
        } finally {
          ShellCommand.execCmd("rm -rf /tmp/carbondata/store")
          ShellCommand.execCmd("rm -rf /tmp/carbondata/meta")
        }

      }
    }
  }

}

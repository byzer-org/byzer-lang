package streaming.core

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}

/**
  * Created by allwefantasy on 26/4/2018.
  */
class DslSpec extends BasicSparkOperation {
  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )

  "set grammar" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      val spark = runtime.sparkSession

      var sq = new ScriptSQLExecListener(spark, "/tmp/william", Map())
      ScriptSQLExec.parse(s""" set hive.exec.dynamic.partition.mode=nonstric options type = "conf" and jack = "" ; """, sq)
      assume(sq.env().contains("hive.exec.dynamic.partition.mode"))

      sq = new ScriptSQLExecListener(spark, "/tmp/william", Map())
      ScriptSQLExec.parse(s""" set  xx = `select unix_timestamp()` options type = "sql" ; """, sq)
      assume(sq.env()("xx").toInt > 0)

      sq = new ScriptSQLExecListener(spark, "/tmp/william", Map())
      ScriptSQLExec.parse(s""" set  xx = "select unix_timestamp()"; """, sq)
      assume(sq.env()("xx") == "select unix_timestamp()")

    }
  }

  "connect grammar" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      val spark = runtime.sparkSession

      var sq = new ScriptSQLExecListener(spark, "/tmp/william", Map())
      ScriptSQLExec.parse("connect jdbc where driver=\"com.mysql.jdbc.Driver\"\nand url=\"jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false\"\nand driver=\"com.mysql.jdbc.Driver\"\nand user=\"root\"\nand password=\"csdn.net\"\nas tableau;\n\nselect \"a\" as a,\"b\" as b\nas tod_boss_dashboard_sheet_1;\n\nsave append tod_boss_dashboard_sheet_1\nas jdbc.`tableau.tod_boss_dashboard_sheet_1`\noptions truncate=\"true\";", sq)

    }
  }

}

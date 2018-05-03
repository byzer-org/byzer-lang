package streaming.core

import java.io.{File, FileFilter}
import java.sql.{Driver, DriverManager}

import net.sf.json.JSONObject
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}

/**
  * Created by allwefantasy on 26/4/2018.
  */
class DslSpec extends BasicSparkOperation with SpecFunctions {
  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )


  "set grammar" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      ScriptSQLExec.parse(s""" set hive.exec.dynamic.partition.mode=nonstric options type = "conf" and jack = "" ; """, sq)
      assume(sq.env().contains("hive.exec.dynamic.partition.mode"))

      sq = createSSEL
      ScriptSQLExec.parse(s""" set  xx = `select unix_timestamp()` options type = "sql" ; """, sq)
      assume(sq.env()("xx").toInt > 0)

      sq = createSSEL
      ScriptSQLExec.parse(s""" set  xx = "select unix_timestamp()"; """, sq)
      assume(sq.env()("xx") == "select unix_timestamp()")

    }
  }



  "save mysql with update" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      //注册表连接
      var sq = createSSEL
      ScriptSQLExec.parse("connect jdbc where driver=\"com.mysql.jdbc.Driver\"\nand url=\"jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false\"\nand driver=\"com.mysql.jdbc.Driver\"\nand user=\"root\"\nand password=\"csdn.net\"\nas tableau;", sq)

      sq = createSSEL
      ScriptSQLExec.parse("select \"a\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      jdbc("drop table tod_boss_dashboard_sheet_1")

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save append tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_1`
           |options truncate="true"
           |and idCol="a,b"
           |and createTableColumnTypes="a VARCHAR(128),b VARCHAR(128)";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_1` as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save append tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_1`
           |options idCol="a,b";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_1` as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

      sq = createSSEL
      ScriptSQLExec.parse("select \"k\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save append tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_1`
           |;
           |load jdbc.`tableau.tod_boss_dashboard_sheet_1` as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 2)
    }
  }

  "save mysql with default" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      //注册表连接
      var sq = createSSEL
      ScriptSQLExec.parse("connect jdbc where driver=\"com.mysql.jdbc.Driver\"\nand url=\"jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false\"\nand driver=\"com.mysql.jdbc.Driver\"\nand user=\"root\"\nand password=\"csdn.net\"\nas tableau;", sq)

      sq = createSSEL
      ScriptSQLExec.parse("select \"a\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save overwrite tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2`
           |options truncate="false";
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save append tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2`
           |;
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 2)

    }
  }

  //  "insert with variable" should "work fine" in {
  //
  //    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
  //      //执行sql
  //      implicit val spark = runtime.sparkSession
  //
  //      var sq = createSSEL
  //      sq = createSSEL
  //      ScriptSQLExec.parse("select \"a\" as a,\"b\" as b\n,\"c\" as c\nas tod_boss_dashboard_sheet_1;", sq)
  //
  //      sq = createSSEL
  //      ScriptSQLExec.parse("set hive.exec.dynamic.partition.mode=nonstric options type = \"conf\" ;" +
  //        "set HADOOP_DATE_YESTERDAY=`2017-01-02` ;" +
  //        "INSERT OVERWRITE TABLE default.abc partition (hp_stat_date = '${HADOOP_DATE_YESTERDAY}') " +
  //        "select * from tod_boss_dashboard_sheet_1;", sq)
  //
  //    }
  //  }

  "analysis with dic" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      ScriptSQLExec.parse(scriptStr("token-analysis"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String]
          == "天了噜/userDefine").size > 0)
    }
  }

  "analysis with dic with n nature include" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      ScriptSQLExec.parse(scriptStr("token-analysis-include-n"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").size() == 1)
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String]
          == "天才/n").size > 0)
    }
  }

  "extract with dic" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      sq = createSSEL
      ScriptSQLExec.parse(scriptStr("token-extract"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String].
          startsWith("天了噜")).size > 0)
    }
  }

  "save with file num options" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(scriptStr("save-filenum"), sq)
      assume(new File("/tmp/william/tmp/abc/").list().filter(f => f.endsWith(".json")).size == 3)
    }
  }

}

package streaming.core

import java.io.File

import net.sf.json.JSONObject
import org.apache.commons.io.{FileUtils}
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 26/4/2018.
  */
class DslSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  "set grammar" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      ScriptSQLExec.parse(s""" set hive.exec.dynamic.partition.mode=nonstric options type = "conf" and jack = "" ; """, sq)
      assert(sq.env().contains("hive.exec.dynamic.partition.mode"))

      sq = createSSEL
      ScriptSQLExec.parse(s""" set  xx = `select unix_timestamp()` options type = "sql" ; """, sq)
      assert(sq.env()("xx").toInt > 0)

      sq = createSSEL
      ScriptSQLExec.parse(s""" set  xx = "select unix_timestamp()"; """, sq)
      assert(sq.env()("xx") == "select unix_timestamp()")

    }
  }

  "set grammar case 2" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession

      var sq = createSSEL
      ScriptSQLExec.parse(""" set a = "valuea"; set b = "${a}/b"; """, sq)
      assert(sq.env()("b") == "valuea/b")

      sq = createSSEL
      ScriptSQLExec.parse(""" set b = "${a}/b"; set a = "valuea";  """, sq)
      assert(sq.env()("b") == "valuea/b")

    }
  }



  "save mysql with update" should "work fine" taggedAs (NotToRunTag) in {

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

  "save mysql with default" should "work fine" taggedAs (NotToRunTag) in {

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

      sq = createSSEL
      ScriptSQLExec.parse(
        s"""
           |save overwrite tod_boss_dashboard_sheet_1
           |as jdbc.`tableau.tod_boss_dashboard_sheet_2` options truncate="true"
           |;
           |load jdbc.`tableau.tod_boss_dashboard_sheet_2` as tbs;
         """.stripMargin, sq)

      assume(spark.sql("select * from tbs").toJSON.collect().size == 1)

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

  def createFile(path: String, content: String) = {
    val f = new File("/tmp/abc.txt")
    if (!f.exists()) {
      FileUtils.write(f, "天了噜", "utf-8")
    }
  }

  "analysis with dic" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"

      val f = new File("/tmp/abc.txt")
      if (!f.exists()) {
        FileUtils.write(f, "天了噜", "utf-8")
      }

      var sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("token-analysis"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String]
          == "天了噜/userDefine").size > 0)
    }
  }

  "analysis with dic and deduplicate" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("token-analysis-deduplicate"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String]
          == "天了噜/userDefine").size == 1)
    }
  }

  "analysis with dic with n nature include" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("token-analysis-include-n"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").size() == 1)
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String]
          == "天才/n").size > 0)
    }
  }

  "extract with dic" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("token-extract"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String].
          startsWith("天了噜")).size > 0)
    }
  }

  "save with file num options" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("save-filenum"), sq)
      assume(new File("/tmp/william/tmp/abc/").list().filter(f => f.endsWith(".json")).size == 3)
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

  "script-support-drop" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParamsWithCarbondata, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
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

//  "SQLJDBC" should "work fine" taggedAs (NotToRunTag) in {
//
//    withBatchContext(setupBatchContext(batchParamsWithCarbondata, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
//      //执行sql
//      implicit val spark = runtime.sparkSession
//      val sq = createSSEL
//
//      ScriptSQLExec.parse(
//        """
//          |connect jdbc where
//          |driver="com.mysql.jdbc.Driver"
//          |and url="jdbc:mysql://127.0.0.1:3306/wow"
//          |and driver="com.mysql.jdbc.Driver"
//          |and user="root"
//          |and password="----"
//          |as mysql1;
//          |select 1 as t as fakeTable;
//          |train fakeTable JDBC.`mysql1` where
//          |driver-statement-0="drop table test1"
//        """.stripMargin, sq)
//    }
//  }

}






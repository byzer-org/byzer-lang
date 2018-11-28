package streaming.test.dsl

import java.io.File

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, NotToRunTag, SpecFunctions}
import streaming.dsl.auth.TableType
import streaming.dsl.{GrammarProcessListener, ScriptSQLExec}

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


  "save with file num options" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("save-filenum"), sq)
      assume(new File("/tmp/william/tmp/abc/").list().filter(f => f.endsWith(".json")).size == 3)
    }
  }


  "ScalaScriptUDF" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams)) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      ScriptSQLExec.parse(
        """
          |/*
          |  MLSQL脚本完成UDF注册示例
          |*/
          |
          |-- 填写script脚本
          |set plusFun='''
          |class PlusFun{
          |  def plusFun(a:Double,b:Double)={
          |   a + b
          |  }
          |}
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScalaScriptUDF.`scriptTable` as plusFun options
          |className="PlusFun"
          |and methodName="plusFun"
          |;
          |
          |-- 使用plusFun
          |select plusFun(1,1) as res as output;
        """.stripMargin, sq)
      var res = spark.sql("select * from output").collect().head.get(0)
      assume(res == 2)


      ScriptSQLExec.parse(
        """
          |/*
          |  MLSQL脚本完成UDF注册示例
          |*/
          |
          |-- 填写script脚本
          |set plusFun='''
          |def plusFun(a:Double,b:Double)={
          |   a + b
          |}
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScalaScriptUDF.`scriptTable` as plusFun options
          |and methodName="plusFun"
          |;
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |-- 使用plusFun
          |select plusFun(1,1) as res from dataTable as output;
        """.stripMargin, sq)
      res = spark.sql("select * from output").collect().head.get(0)
      assume(res == 2)
      res = spark.sql("select plusFun(1,1)").collect().head.get(0)
      assume(res == 2)

      ScriptSQLExec.parse(
        """
          |/*
          |  MLSQL脚本完成UDF注册示例
          |*/
          |
          |-- 填写script脚本
          |set plusFun='''
          |def apply(a:Double,b:Double)={
          |   a + b
          |}
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScalaScriptUDF.`scriptTable` as plusFun
          |;
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |-- 使用plusFun
          |select plusFun(1,1) as res from dataTable as output;
        """.stripMargin, sq)

      res = spark.sql("select plusFun(1,1)").collect().head.get(0)
      assume(res == 2)
    }
  }
  "pyton udf" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams)) { runtime: SparkRuntime =>

      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse(
        """
          |
          |-- 填写script脚本
          |set plusFun='''
          |def plusFun(self,a,b,c,d,e,f):
          |    return a+b+c+d+e+f
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScriptUDF.`scriptTable` as plusFun options
          |and methodName="plusFun"
          |and lang="python"
          |and dataType="integer"
          |;
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |-- 使用plusFun
          |select plusFun(1, 2, 3, 4, 5, 6) as res from dataTable as output;
        """.stripMargin, sq)
      var res = spark.sql("select * from output").collect().head.get(0)
      assume(res == 21)

      ScriptSQLExec.parse(
        """
          |
          |-- 填写script脚本
          |set plusFun='''
          |def plusFun(self,m):
          |    return "-".join(m)
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScriptUDF.`scriptTable` as plusFun options
          |and methodName="plusFun"
          |and lang="python"
          |and dataType="string"
          |;
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |-- 使用plusFun
          |select plusFun(array('a','b')) as res from dataTable as output;
        """.stripMargin, sq)
      res = spark.sql("select * from output").collect().head.get(0)
      assume(res == "a-b")
      ScriptSQLExec.parse(
        """
          |
          |-- 填写script脚本
          |set plusFun='''
          |def plusFun(self,m):
          |    return m
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScriptUDF.`scriptTable` as plusFun options
          |and methodName="plusFun"
          |and lang="python"
          |and dataType="map(string,string)"
          |;
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |-- 使用plusFun
          |select plusFun(map('a','b')) as res from dataTable as output;
        """.stripMargin, sq)
      res = spark.sql("select * from output").collect().head.get(0)
      assume(res.asInstanceOf[Map[String, String]]("a") == "b")

      ScriptSQLExec.parse(
        """
          |
          |-- 填写script脚本
          |set plusFun='''
          |def apply(self,m):
          |    return m
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScriptUDF.`scriptTable` as plusFun options
          |and lang="python"
          |and dataType="map(string,string)"
          |;
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |-- 使用plusFun
          |select plusFun(map('a','b')) as res from dataTable as output;
        """.stripMargin, sq)
      res = spark.sql("select * from output").collect().head.get(0)
      assume(res.asInstanceOf[Map[String, String]]("a") == "b")

      ScriptSQLExec.parse(
        """
          |
          |register ScriptUDF.`` as count_board options lang="python"
          |    and methodName="apply"
          |    and dataType="map(string,integer)"
          |    and code='''
          |def apply(self, s):
          |    from collections import Counter
          |    return dict(Counter(s))
          |    '''
          |;
          |select count_board(array("你好","我好","大家好","你好")) as c as output;
        """.stripMargin, sq)
      res = spark.sql("select * from output").toJSON.collect().head
      assume(res == "{\"c\":{\"我好\":1,\"你好\":2,\"大家好\":1}}")

    }
  }


  "include" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      val source =
        """
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScalaScriptUDF.`scriptTable` as plusFun options
          |className="PlusFun"
          |and methodName="plusFun"
          |;
          |
          |-- 使用plusFun
          |select plusFun(1,1) as res as output;
        """.stripMargin
      writeStringToFile("/tmp/william/tmp/kk.jj", source)
      ScriptSQLExec.parse(
        """
          |-- 填写script脚本
          |set plusFun='''
          |class PlusFun{
          |  def plusFun(a:Double,b:Double)={
          |   a + b
          |  }
          |}
          |''';
          |include hdfs.`/tmp/kk.jj`;
        """.stripMargin
        , sq, false)
      val res = spark.sql("select * from output").collect().head.get(0)
      assume(res == 2)
    }
  }

  "include set" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      var source =
        """
          |set a="b" options type="defaultParam";
          |
        """.stripMargin
      writeStringToFile("/tmp/william/tmp/kk.jj", source)
      ScriptSQLExec.parse(
        """
          |set a="c";
          |include hdfs.`/tmp/kk.jj`;
          |select "${a}" as res as display;
        """.stripMargin
        , sq, false)
      var res = spark.sql("select * from display").collect().head.get(0)
      assume(res == "c")

      sq = createSSEL
      source =
        """
          |set a="b";
          |
        """.stripMargin
      writeStringToFile("/tmp/william/tmp/kk.jj", source)
      ScriptSQLExec.parse(
        """
          |set a="c";
          |include hdfs.`/tmp/kk.jj`;
          |select "${a}" as res as display;
        """.stripMargin
        , sq, false)
      res = spark.sql("select * from display").collect().head.get(0)
      assume(res == "b")
    }
  }

  "train or run" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      val source =
        """
          |
          |set data='''
          |{"a":"c"}
          |{"a":"a"}
          |{"a":"k"}
          |{"a":"g"}
          |''';
          |load jsonStr.`data` as dataTable;
          |run dataTable as StringIndex.`/tmp/model1` where inputCol="a";
          |train dataTable as StringIndex.`/tmp/model2` where inputCol="a";
          |train dataTable as StringIndex.`/tmp/model3` options inputCol="a";
        """.stripMargin
      ScriptSQLExec.parse(source, sq, false)

      val source1 =
        """
          |
          |set data='''
          |{"a":"c"}
          |{"a":"a"}
          |{"a":"k"}
          |{"a":"g"}
          |''';
          |load jsonStr.`data` as dataTable;
          |run dataTable where inputCol="a" as StringIndex.`/tmp/model1`;
        """.stripMargin
      val res = intercept[RuntimeException](ScriptSQLExec.parse(source1, sq, false)).getMessage
      assume(res.startsWith("MLSQL Parser error"))
    }
  }

  "save-partitionby" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val ssel = createSSEL
      val sq = new GrammarProcessListener(ssel)
      intercept[RuntimeException] {
        // Result type: IndexOutOfBoundsException
        ScriptSQLExec.parse("save jack append skone_task_log\nas parquet.`${data_monitor_skone_task_log_2_parquet_data_path}`\noptions mode = \"Append\"\nand duration = \"10\"\nand checkpointLocation = \"${data_monitor_skone_task_log_2_parquet_checkpoint_path}\"\npartitionBy hp_stat_date;", sq)
      }

      ScriptSQLExec.parse("save append skone_task_log\nas parquet.`${data_monitor_skone_task_log_2_parquet_data_path}`\noptions mode = \"Append\"\nand duration = \"10\"\nand checkpointLocation = \"${data_monitor_skone_task_log_2_parquet_checkpoint_path}\"\npartitionBy hp_stat_date;", sq)

    }
  }

  "load" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val ssel = createSSEL
      val sq = new GrammarProcessListener(ssel)
      ScriptSQLExec.parse("load parquet.`/tmp/abc` as newtable;", sq)
    }
  }

  "load-exaplain" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val ssel = createSSEL

      ScriptSQLExec.parse("load model.`list` as output;", ssel)
      spark.sql("select * from output").show()
      ScriptSQLExec.parse("""load model.`params` where alg="RandomForest" as output;""", ssel)
      spark.sql("select * from output").show()
      ScriptSQLExec.parse("""load model.`example` where alg="RandomForest" as output;""", ssel)
      spark.sql("select * from output").show()

      ScriptSQLExec.parse("load workflow.`list` as output;", ssel)
      spark.sql("select * from output").show()
      ScriptSQLExec.parse("load workflow.`` as output;", ssel)
      spark.sql("select * from output").show()

      ScriptSQLExec.parse("load workflow.`types` as output;", ssel)
      spark.sql("select * from output").show()
    }
  }

  "auth-1" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val ssel = createSSEL
      val mlsql =
        """
          |set auth_client="streaming.dsl.auth.meta.client.DefaultConsoleClient";
          |load parquet.`/tmp/abc` as newtable;
          |select * from default.abc as cool;
        """.stripMargin
      withClue("auth fail") {
        assertThrows[RuntimeException] {
          ScriptSQLExec.parse(mlsql, ssel, true, false)
        }
      }
      val hiveTable = ssel.authProcessListner.get.tables().tables.filter(f => f.tableType == TableType.HIVE).head
      assume(hiveTable.table.get == "abc")
      val tempTable = ssel.authProcessListner.get.tables().tables.filter(f => f.tableType == TableType.TEMP).map(f => f.table.get).toSet
      assume(tempTable == Set("cool", "newtable"))
      val hdfsTable = ssel.authProcessListner.get.tables().tables.filter(f => f.tableType == TableType.HDFS).map(f => f.table.get).toSet
      assume(hdfsTable == Set("/tmp/abc"))
    }
  }

  "load save support variable" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val ssel = createSSEL
      val mlsql =
        """
          |set table1="jack";
          |select "a" as a as `${table1}`;
          |select *  from jack as output;
        """.stripMargin
      ScriptSQLExec.parse(mlsql, ssel)
      assume(spark.sql("select * from output").collect().map(f => f.getAs[String](0)).head == "a")
      val mlsql2 =
        """
          |set table1="jack";
          |select "a" as a as `${table1}`;
          |save overwrite `${table1}` as parquet.`/tmp/jack`;
          |load parquet.`/tmp/jack` as jackm;
          |select *  from jackm as output;
        """.stripMargin
      ScriptSQLExec.parse(mlsql2, ssel)
      assume(spark.sql("select * from output").collect().map(f => f.getAs[String](0)).head == "a")

    }
  }

}






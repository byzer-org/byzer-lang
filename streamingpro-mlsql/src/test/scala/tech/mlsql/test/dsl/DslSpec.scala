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

package tech.mlsql.test.dsl

import java.io.File

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.streaming.BasicSparkOperation
import streaming.common.PathFun
import streaming.common.shell.ShellCommand
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, NotToRunTag, SpecFunctions}
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.dsl.processor.GrammarProcessListener

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

  "save with partition by options" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ShellCommand.execCmd("rm -rf /tmp/william/tmp/abc")
      ScriptSQLExec.parse(
        s"""
           |select 1 as jack,2 as bj,3 as kk as table1;
           |save overwrite  table1 as parquet.`/tmp/abc` partitionBy jack, bj;
         """.stripMargin, sq)
      assume(new File("/tmp/william/tmp/abc/jack=1/bj=2").exists())

      ScriptSQLExec.parse(
        s"""
           |select 1 as jack,2 as bj,3 as kk as table1;
           |save overwrite  table1 as parquet.`/tmp/abc` partitionBy jack;
         """.stripMargin, sq)
      assume(new File("/tmp/william/tmp/abc/jack=1/").list().filter(f => f.endsWith(".parquet")).size > 0)
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

      ScriptSQLExec.parse(
        """
          |register ScriptUDF.scriptTable as dateRange options
          |and lang="python"
          |and dataType="array(string)"
          |and code='''
          |def apply(self, begin_time, end_time):
          |    import datetime
          |    range_list = ['a', 'b']
          |    return range_list
          |''';
          |
          |select dateRange(1544715389, 1544715390) as a as outer;
        """.stripMargin, sq)
      spark.sql("select * from outer").show()

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
    }
  }


  "load save support variable" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) {
      runtime: SparkRuntime =>
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

  "load api" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) {
      runtime: SparkRuntime =>
        //执行sql
        implicit val spark = runtime.sparkSession
        mockServer
        val ssel = createSSEL
        val mlsql =
          """
            |load _mlsql_.`api/list` as output;
          """.stripMargin
        ScriptSQLExec.parse(mlsql, ssel)
        spark.sql("select * from output").show()

    }
  }

  "load conf" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) {
      runtime: SparkRuntime =>
        //执行sql
        implicit val spark = runtime.sparkSession

        val ssel = createSSEL
        val mlsql =
          """
            |load _mlsql_.`api/list` as output;
          """.stripMargin
        ScriptSQLExec.parse(mlsql, ssel)
        spark.sql("select * from output").show()

    }
  }

  "Command" should "alias commnd work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) {
      implicit runtime: SparkRuntime =>
        implicit val spark = runtime.sparkSession

        def compareDSL(command: String, targetStr: String) = {
          val ssel = createSSEL
          executeScript(
            s"""
               |set kill=''' run command as Kill.`{}` ''';
               |set jobId="wow";
               |
              |${command};
            """.stripMargin, ssel)
          println(ssel.preProcessListener.get.toScript)
          assert(ssel.preProcessListener.get.toScript == targetStr)
        }

        compareDSL("""!kill "jobId"""","""set kill=''' run command as Kill.`{}` ''';set jobId="wow"; run command as Kill.`jobId` ;""")

        compareDSL("""!kill jobId""","""set kill=''' run command as Kill.`{}` ''';set jobId="wow"; run command as Kill.`jobId` ;""")

        compareDSL("""!kill '''jobId"'''""","""set kill=''' run command as Kill.`{}` ''';set jobId="wow"; run command as Kill.`jobId"` ;""")
        compareDSL("""!kill '''${jobId}'''""","""set kill=''' run command as Kill.`{}` ''';set jobId="wow"; run command as Kill.`wow` ;""")

        val ssel = createSSEL
        executeScript(
          s"""
             |!show;
            """.stripMargin, ssel)
        ssel.preProcessListener.get.toScript should not include ("{}")


    }
  }


  "Command" should "alias expand work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) {
      implicit runtime: SparkRuntime =>
        implicit val spark = runtime.sparkSession

        def compareDSL(command: String, targetStr: String) = {
          val ssel = createSSEL
          executeScript(command, ssel)
          println(ssel.preProcessListener.get.toScript)
          assert(ssel.preProcessListener.get.toScript contains targetStr)
        }

        compareDSL(
          """
            |set vtest = '''
            |run command VTest.`{}` where name="{}"
            |''';
            |!vtest wow jack;
          """.stripMargin, "run command VTest.`wow` where name=\"jack\"")

        compareDSL(
          """
            |set vtest = '''
            |run command VTest.`{}` where name="{}"{}
            |''';
            |!vtest wow jack;
          """.stripMargin, "run command VTest.`wow` where name=\"jack\"")

        compareDSL(
          """
            |set vtest = '''
            |run command VTest.`{1}` where name="{0}"
            |''';
            |!vtest wow jack;
          """.stripMargin, "run command VTest.`jack` where name=\"wow\"")

        compareDSL(
          """
            |set vtest = '''
            |run command VTest.`{}` where name="{}"
            |''';
            |!vtest;
          """.stripMargin, "run command VTest.`` where name=\"\"")

        compareDSL(
          """
            |set vtest = '''
            |run command VTest.`{}` where {1} name="{}"
            |''';
            |!vtest wow jack;
          """.stripMargin, "run command VTest.`wow` where jack name=\"jack\"")

    }
  }

  "mlsql" should "valiate before really executed" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) {
      implicit runtime: SparkRuntime =>

        assertThrows[ParseException] { // Result type: Assertion
          executeScriptWithValidate(
            """
              |select a as b from table1 c m  as jack;
            """.stripMargin)
        }

        val res = executeScriptWithValidate(
          """
            |select a as b from table1 as jack;
          """.stripMargin)
        assert(res != null)
    }
  }

  "path join " should "work fine" in {
    assert("/jack/ow/ab/no.md" == PathFun.joinPath("/jack", "ow", "", "/ab/", "no.md"))
    assert("/jack/ow/ab/no.md" == (PathFun("/jack") / "ow" / "" / "/ab/" / "no.md").toPath)
  }

  def executeScriptWithValidate(script: String)(implicit runtime: SparkRuntime) = {
    implicit val spark = runtime.sparkSession
    val ssel = createSSEL
    ScriptSQLExec.parse(script, ssel, false, true, true, skipGrammarValidate = false)
    ssel
  }

  def executeScript(script: String)(implicit runtime: SparkRuntime) = {
    implicit val spark = runtime.sparkSession
    val ssel = createSSEL
    ScriptSQLExec.parse(script, ssel, false, true, true)
  }

  def executeScript(script: String, ssel: ScriptSQLExecListener)(implicit runtime: SparkRuntime) = {
    implicit val spark = runtime.sparkSession
    ScriptSQLExec.parse(script, ssel, false, true, true)
  }

}


package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.shell.ShellCommand

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLLoadStrSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {

  def executeScript(script: String)(implicit runtime: SparkRuntime) = {
    implicit val spark = runtime.sparkSession
    val ssel = createSSEL
    ScriptSQLExec.parse(script, ssel, true, true, false)
  }

  "load jsonStr" should "[jsonStr] work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession

      executeScript(
        """
          |set data='''
          |{"jack":"yes"}
          |''';
          |load jsonStr.`data` as datasource;
        """.stripMargin)
      assert(spark.sql("select * from datasource").collect().map(f => f.getString(0)).head == "yes")

      val caught = intercept[RuntimeException] {
        executeScript(
          """
            |save overwrite datasource as jsonStr.`/tmp/jack`;
          """.stripMargin)
      }
      assert(caught.getMessage == "save is not supported in jsonStr")

    }
  }

  "load csvStr" should "[csvStr] work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession

      var ssel = createSSEL

      ScriptSQLExec.parse(
        """
          |set data='''
          |a,b,c
          |jack,wow,wow2
          |''';
        """.stripMargin, ssel)

      ScriptSQLExec.parse(
        """
          |load csvStr.`data` where header="true" as datasource;
        """.stripMargin, ssel)
      assert(spark.sql("select * from datasource").collect().map(f => f.getString(0)).head == "jack")

      val caught = intercept[RuntimeException] {
        ScriptSQLExec.parse(
          """
            |save overwrite datasource as csvStr.`/tmp/jack`;
          """.stripMargin, ssel)
      }
      assert(caught.getMessage == "save is not supported in csvStr")

    }
  }

  "load hive" should "[hive] work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      ShellCommand.exec("rm -rf /tmp/user/hive/warehouse/carbon_jack")

      var ssel = createSSEL

      ScriptSQLExec.parse(
        """
          |set data='''
          |a,b,c
          |jack,wow,wow2
          |''';
        """.stripMargin, ssel)

      ScriptSQLExec.parse(
        """
          |load csvStr.`data` where header="true" as datasource;
          |save overwrite datasource as hive.`default.carbon_jack`;
          |load hive.`default.carbon_jack` as newTable;
          |select * from newTable as hiveOutput;
        """.stripMargin, ssel)
      assert(spark.sql("select * from hiveOutput").collect().map(f => f.getString(0)).head == "jack")

    }
  }

  "load csv/json" should "[csv/json] work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession

      executeScript(
        """
          |set data='''
          |a,b,c
          |jack,wow,wow2
          |''';
          |load csvStr.`data` where header="true" as datasource;
          |save overwrite datasource as csv.`/tmp/jack`;
          |load csv.`/tmp/jack` as newTable;
          |select * from newTable as hiveOutput;
        """.stripMargin)
      assert(spark.sql("select * from hiveOutput").collect().map(f => f.getString(0)).head == "jack")
      assert(spark.sql("select * from csv.`/tmp/william/tmp/jack`").collect().map(f => f.getString(0)).head == "jack")


      executeScript(
        """
          |set data='''
          |a,b,c
          |jack,wow,wow2
          |''';
          |load csvStr.`data` where header="true" as datasource;
          |save overwrite datasource as json.`/tmp/jack`;
          |load json.`/tmp/jack` as newTable;
          |select * from newTable as hiveOutput;
        """.stripMargin)
      assert(spark.sql("select * from hiveOutput").collect().map(f => f.getString(0)).head == "jack")
      assert(spark.sql("select * from json.`/tmp/william/tmp/jack`").collect().map(f => f.getString(0)).head == "jack")

    }
  }

}

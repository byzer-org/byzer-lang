package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.log.Logging

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLLoadStrSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
  "load jsonStr" should "[jsonStr] work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession

      var ssel = createSSEL

      ScriptSQLExec.parse(
        """
          |set data='''
          |{"jack":"yes"}
          |''';
        """.stripMargin, ssel)

      ScriptSQLExec.parse(
        """
          |load jsonStr.`data` as datasource;
        """.stripMargin, ssel)
      assert(spark.sql("select * from datasource").collect().map(f => f.getString(0)).head == "yes")

      val caught = intercept[RuntimeException] {
        ScriptSQLExec.parse(
          """
            |save overwrite datasource as jsonStr.`/tmp/jack`;
          """.stripMargin, ssel)
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
          |save overwrite datasource as hive.`default.jack`;
          |load hive.`default.jack` as newTable;
          |select * from newTable as hiveOutput;
        """.stripMargin, ssel)
      assert(spark.sql("select * from hiveOutput").collect().map(f => f.getString(0)).head == "jack")

    }
  }

}

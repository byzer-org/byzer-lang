package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec

/**
 * Created by fchen on 17/1/2019.
 */
class ScriptDataSourceSuite extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "test csv script datasource" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |set rawData='''
          |name,age
          |zhangsan,1
          |lisi,2
          |''';
          |load csvStr.`rawData` options header="true"
          |as output;
        """.stripMargin, sq)

      val result = runtime.sparkSession.sql("select * from output").collect()
      assert(result.size == 2)
      val expectAge = {
        val name = result.head.getAs[String]("name")
        if (name == "zhangsan") {
          "1"
        } else {
          "2"
        }
      }
      assert(result.head.getAs[String]("age") == expectAge)
    }
  }

}

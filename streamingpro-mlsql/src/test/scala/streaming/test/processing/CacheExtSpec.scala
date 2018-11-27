package streaming.test.processing

import org.apache.spark.execution.TestHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec

class CacheExtSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "cache" should "cache or uncache successfully" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |select "a" as a as table1;
          |run table1 as CacheExt.`` where execute="cache";
        """.stripMargin, sq)

      val df = spark.table("table1")
      assume(spark.sharedState.cacheManager.lookupCachedData(df).isDefined)

      ScriptSQLExec.parse(
        """
          |run table1 as CacheExt.`` where execute="uncache";
        """.stripMargin, sq)

      assume(!spark.sharedState.cacheManager.lookupCachedData(df).isDefined)
    }
  }

  "cache" should "supports eager" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |select "a" as a as table1;
          |run table1 as CacheExt.`` where execute="cache" ;
        """.stripMargin, sq)

      var df = spark.table("table1")
      assume(!isCacheBuild(df))

      ScriptSQLExec.parse(
        """
          |select "a" as a as table1;
          |run table1 as CacheExt.`` where execute="cache" and isEager="true";
        """.stripMargin, sq)

      df = spark.table("table1")
      assume(isCacheBuild(df))

    }
  }

  def isCacheBuild(df: DataFrame)(implicit spark: SparkSession) = {
    TestHelper.isCacheBuild(df)(spark)
  }

}

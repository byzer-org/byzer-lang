package streaming.dsl.mmlib.algs

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.BasicMLSQLConfig
import streaming.core.strategy.platform.SparkRuntime
import tech.mlsql.common.utils.log.Logging


case class SQLRateSamplerModel(features: Seq[Double], label: Double)

class SQLRateSamplerTestSuite extends BasicSparkOperation with BasicMLSQLConfig with Logging {
  "Sum of sampleRate elements not equals to 1" should " produce RuntimeException" in {
    assertThrows[RuntimeException] {
      withContext(setupBatchContext(mlsqlParams)) { runtime : SparkRuntime =>
        val spark = getSession(runtime)
        import spark.implicits._
        val sqlRateSampler = new SQLRateSampler
        val df = Seq(SQLRateSamplerModel(Seq(5.1,3.5,1.4,0.2), 0.1)).toDF

        // Normal case
        val params2 = Map(sqlRateSampler.sampleRate.name -> "0.3,0.7",
          sqlRateSampler.labelCol.name -> "label")
        sqlRateSampler.train(df, "", params2)
        // sum of sampleRate not equals to 1
        val params = Map(sqlRateSampler.sampleRate.name -> "0.2,0.7",
          sqlRateSampler.labelCol.name -> "label")
        sqlRateSampler.train(df, "", params)
      }
    }
  }

  "3 sampleRates " should " produce RuntimeException " in {
    assertThrows[RuntimeException] {
      withContext(setupBatchContext(batchParamsWithoutHive)) { runtime : SparkRuntime =>
        val spark = getSession(runtime)
        import spark.implicits._
        val sqlRateSampler = new SQLRateSampler
        val df = Seq(SQLRateSamplerModel(Seq(5.1,3.5,1.4,0.2), 0.1)).toDF
        // 3 sampleRates
        val params3 = Map(sqlRateSampler.sampleRate.name -> "0.3,0.1,0.9",
          sqlRateSampler.labelCol.name -> "label")
        sqlRateSampler.train(df, "", params3)
      }
    }
  }
}

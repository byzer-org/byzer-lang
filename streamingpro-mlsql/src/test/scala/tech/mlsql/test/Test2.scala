package tech.mlsql.test

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}

/**
 * 10/11/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class Test2 extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  "data-drive" should "framework" in {
    withContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      val session = runtime.sparkSession
      //RDD
      session.sparkContext.
        parallelize(Seq(10), 2).
        mapPartitions { iter =>
        iter.foreach(item => println(item))
        iter
      }.
        collect()


    }
  }


}



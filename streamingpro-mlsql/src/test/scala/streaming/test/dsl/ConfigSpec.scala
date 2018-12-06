package streaming.test.dsl

import java.util

import org.apache.spark.MLSQLConf
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.{BasicMLSQLConfig, SpecFunctions}

import scala.collection.JavaConversions._

/**
  * 2018-11-30 WilliamZhu(allwefantasy@gmail.com)
  */
class ConfigSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "MLSQLConf" should "get streaming.platform right" in {
    val params = new util.HashMap[Any, Any]()
    params.put("streaming.platform", "spark")

    val configReader = MLSQLConf.createConfigReader(params.map(f => (f._1.toString, f._2.toString)))
    assume(MLSQLConf.MLSQL_PLATFORM.readFrom(configReader).get == "spark")
  }

  "MLSQLConf" should "get default value right" in {
    val params = new util.HashMap[Any, Any]()
    val configReader = MLSQLConf.createConfigReader(params.map(f => (f._1.toString, f._2.toString)))
    assume(MLSQLConf.MLSQL_NAME.readFrom(configReader) == "mlsql")
  }

  "MLSQLConf" should "auto convert type right" in {
    val params = new util.HashMap[Any, Any]()
    params.put(MLSQLConf.MLSQL_SPARK_SERVICE.key, "true")
    val configReader = MLSQLConf.createConfigReader(params.map(f => (f._1.toString, f._2.toString)))
    assume(MLSQLConf.MLSQL_SPARK_SERVICE.readFrom(configReader))
  }
}

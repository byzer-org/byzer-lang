package tech.mlsql.test.dsl

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import tech.mlsql.common.utils.base.Templates

/**
 * 25/8/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TemplateSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "named template" should "work" in {
    println(Templates.evaluate(" hello {} ",Seq("jack")))
  }
}

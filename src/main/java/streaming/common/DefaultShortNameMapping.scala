package streaming.common

import serviceframework.dispatcher.ShortNameMapping

/**
 * 11/21/16 WilliamZhu(allwefantasy@gmail.com)
 */

class DefaultShortNameMapping extends ShortNameMapping {
  private val compositorNameMap: Map[String, String] = Map[String, String](
    "spark" -> "streaming.core.strategy.SparkStreamingStrategy",

    "batch.source" -> "streaming.core.compositor.spark.source.SQLSourceCompositor",
    "batch.sql" -> "streaming.core.compositor.spark.transformation.SQLCompositor",
    "batch.table" -> "streaming.core.compositor.spark.transformation.JSONTableCompositor",
    "batch.refTable" -> "streaming.core.compositor.spark.transformation.JSONRefTableCompositor",

    "stream.source.kafka" -> "streaming.core.compositor.spark.streaming.source.KafkaStreamingCompositor",
    "stream.sql" -> "streaming.core.compositor.spark.streaming.transformation.SQLCompositor",
    "stream.table" -> "streaming.core.compositor.spark.streaming.transformation.JSONTableCompositor",
    "stream.refTable" -> "streaming.core.compositor.spark.streaming.transformation.JSONRefTableCompositor",

    "ss.source" -> "streaming.core.compositor.spark.ss.source.SQLSourceCompositor",
    "ss.source.mock" -> "streaming.core.compositor.spark.ss.source.MockSQLSourceCompositor",
    "ss.sql" -> "streaming.core.compositor.spark.transformation.SQLCompositor",
    "ss.table" -> "streaming.core.compositor.spark.transformation.JSONTableCompositor",
    "ss.refTable" -> "streaming.core.compositor.spark.transformation.JSONRefTableCompositor",
    "ss.output" -> "streaming.core.compositor.spark.ss.output.SQLOutputCompositor"
  )

  override def forName(shortName: String): String = {
    if (compositorNameMap.contains(shortName)) {
      compositorNameMap(shortName)
    } else {
      shortName
    }
  }
}
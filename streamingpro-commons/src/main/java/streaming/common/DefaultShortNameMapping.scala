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

package streaming.common

import serviceframework.dispatcher.ShortNameMapping

/**
  * 11/21/16 WilliamZhu(allwefantasy@gmail.com)
  */

class DefaultShortNameMapping extends ShortNameMapping {
  private val compositorNameMap: Map[String, String] = Map[String, String](
    "spark" -> "streaming.core.strategy.SparkStreamingStrategy",
    "refTable" -> "streaming.core.strategy.SparkStreamingRefStrategy",
    "refFunction" -> "streaming.core.strategy.SparkStreamingRefStrategy",
    "flink" -> "streaming.core.strategy.SparkStreamingStrategy",

    "sql.udf" -> "streaming.core.compositor.spark.udf.SQLUDFCompositor",

    "batch.source" -> "streaming.core.compositor.spark.source.SQLSourceCompositor",
    "batch.sources" -> "streaming.core.compositor.spark.source.MultiSQLSourceCompositor",
    "batch.sql" -> "streaming.core.compositor.spark.transformation.SQLCompositor",
    "batch.table" -> "streaming.core.compositor.spark.transformation.JSONTableCompositor",
    "batch.refTable" -> "streaming.core.compositor.spark.transformation.JSONRefTableCompositor",
    "batch.script" -> "streaming.core.compositor.spark.transformation.ScriptCompositor",
    "batch.mlsql" -> "streaming.core.compositor.spark.transformation.MLSQLCompositor",
    "batch.script.df" -> "streaming.core.compositor.spark.transformation.DFScriptCompositor",
    "batch.row.index" -> "streaming.core.compositor.spark.transformation.RowNumberCompositor",
    "batch.columns" -> "streaming.core.compositor.spark.transformation.SingleColumnJSONCompositor",
    "batch.output" -> "streaming.core.compositor.spark.output.SQLOutputCompositor",
    "batch.persist" -> "streaming.core.compositor.spark.persist.PersistCompositor",
    "batch.unpersist" -> "streaming.core.compositor.spark.persist.UnpersistCompositor",

    "batch.output.alg" -> "streaming.core.compositor.spark.output.AlgorithmOutputCompositor",
    "batch.alg" -> "streaming.core.compositor.spark.transformation.AlgorithmCompositor",

    "batch.output.console" -> "streaming.core.compositor.spark.output.SQLPrintCompositor",
    "batch.outputs" -> "streaming.core.compositor.spark.output.MultiSQLOutputCompositor",

    "stream.source.kafka" -> "streaming.core.compositor.spark.streaming.source.KafkaStreamingCompositor",
    "stream.sources.kafka" -> "streaming.core.compositor.spark.streaming.source.MultiKafkaStreamingCompositor",
    "stream.sources" -> "streaming.core.compositor.spark.streaming.source.MultiSQLSourceCompositor",
    "stream.sql" -> "streaming.core.compositor.spark.streaming.transformation.SQLCompositor",
    "stream.table" -> "streaming.core.compositor.spark.streaming.transformation.JSONTableCompositor",
    "stream.refTable" -> "streaming.core.compositor.spark.transformation.JSONRefTableCompositor",
    "stream.columns" -> "streaming.core.compositor.spark.streaming.transformation.SingleColumnJSONCompositor",
    "stream.source.mock.json" -> "streaming.core.compositor.spark.streaming.source.MockInputStreamCompositor",
    "stream.output" -> "streaming.core.compositor.spark.streaming.output.SQLOutputCompositor",
    "stream.outputs" -> "streaming.core.compositor.spark.streaming.output.MultiSQLOutputCompositor",
    "stream.output.csv" -> "streaming.core.compositor.spark.streaming.output.SQLCSVOutputCompositor",
    "stream.output.parquet" -> "streaming.core.compositor.spark.streaming.output.SQLParquetOutputCompositor",
    "stream.output.es" -> "streaming.core.compositor.spark.streaming.output.SQLESOutputCompositor",
    "stream.script.df" -> "streaming.core.compositor.spark.streaming.transformation.DFScriptCompositor",
    "stream.output.carbondata" -> "streaming.core.compositor.spark.streaming.output.CarbonDataOutputCompositor",
    "stream.output" -> "streaming.core.compositor.spark.streaming.output.MultiSQLOutputCompositor",
    "stream.output.console" -> "streaming.core.compositor.spark.streaming.output.ConsoleOutputCompositor",
    "stream.output.unittest" -> "streaming.core.compositor.spark.streaming.output.SQLUnitTestCompositor",
    "stream.output.print" -> "streaming.core.compositor.spark.streaming.output.SQLPrintOutputCompositor",

    "ss.sources" -> "streaming.core.compositor.spark.ss.source.MultiSQLSourceCompositor",
    "ss.sql" -> "streaming.core.compositor.spark.ss.transformation.SQLCompositor",
    "ss.outputs" -> "streaming.core.compositor.spark.ss.output.MultiSQLOutputCompositor",

    "init" -> "tech.mlsql.compositor.InitializationCompositor",

    "flink.sources" -> "streaming.core.compositor.flink.streaming.source.MultiStreamingCompositor",
    "flink.sql" -> "streaming.core.compositor.flink.streaming.transformation.SQLCompositor",
    "flink.outputs" -> "streaming.core.compositor.flink.streaming.output.MultiSQLOutputCompositor"
  )

  override def forName(shortName: String): String = {
    if (compositorNameMap.contains(shortName)) {
      compositorNameMap(shortName)
    } else {
      shortName
    }
  }
}
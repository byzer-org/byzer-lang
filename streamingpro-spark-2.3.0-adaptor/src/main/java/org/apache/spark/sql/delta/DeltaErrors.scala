/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.io.FileNotFoundException
import java.util.ConcurrentModificationException

import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataType, StructField, StructType}

trait DocsPath {
  /**
    * The URL for the base path of Delta's docs.
    */
  def baseDocsPath(conf: SparkConf): String = "https://docs.delta.io"
}

/**
  * A holder object for Delta errors.
  */
object DeltaErrors
  extends DocsPath {

  def baseDocsPath(spark: SparkSession): String = baseDocsPath(spark.sparkContext.getConf)

  val DeltaSourceIgnoreDeleteErrorMessage =
    "Detected deleted data from streaming source. This is currently not supported. If you'd like " +
      "to ignore deletes, set the option 'ignoreDeletes' to 'true'."

  val DeltaSourceIgnoreChangesErrorMessage =
    "Detected a data update in the source table. This is currently not supported. If you'd " +
      "like to ignore updates, set the option 'ignoreChanges' to 'true'. If you would like the " +
      "data update to be reflected, please restart this query with a fresh checkpoint directory."

  def illegalDeltaOptionException(name: String, input: String, explain: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value '$input' for option '$name', $explain")
  }
  def pathNotSpecifiedException: Throwable = {
    new IllegalArgumentException("'path' is not specified")
  }
  
}

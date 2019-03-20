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

package streaming.core.datasource

import _root_.streaming.dsl.MLSQLExecuteContext
import org.apache.spark.sql._

/**
  * 2018-12-20 WilliamZhu(allwefantasy@gmail.com)
  */

trait MLSQLDataSource {
  def dbSplitter = {
    "."
  }

  def fullFormat: String

  def shortFormat: String

  def aliasFormat: String = {
    shortFormat
  }

}

trait MLSQLSource extends MLSQLDataSource with MLSQLSourceInfo {
  def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame
}

trait RewriteableSource {
  def rewrite(df: DataFrame,
              config: DataSourceConfig,
              sourceInfo: Option[SourceInfo],
              context: MLSQLExecuteContext): DataFrame
}

trait MLSQLSink extends MLSQLDataSource {
  def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any
}

trait MLSQLDirectSource extends MLSQLDataSource {
  def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame
}

trait MLSQLDirectSink extends MLSQLDataSource {
  def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any
}

case class SourceInfo(sourceType: String, db: String, table: String)

trait MLSQLSourceInfo extends MLSQLDataSource {
  def sourceInfo(config: DataAuthConfig): SourceInfo

  def explainParams(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.createDataset[String](Seq()).toDF("name")
  }
}


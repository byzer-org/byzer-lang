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

package org.apache.spark.sql.execution.streaming.newfile

import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.streaming.{FileStreamSink, Sink}
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by allwefantasy on 22/5/2018.
  */
class DefaultSource extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val path = parameters.getOrElse("path", {
      throw new IllegalArgumentException("'path' is not specified")
    })
    if (outputMode != OutputMode.Append) {
      throw new AnalysisException(
        s"Data source ${getClass.getCanonicalName} does not support $outputMode output mode")
    }
    new NewFileStreamSink(sqlContext.sparkSession, parameters("path"), new ParquetFileFormat(), partitionColumns, parameters)
  }
}

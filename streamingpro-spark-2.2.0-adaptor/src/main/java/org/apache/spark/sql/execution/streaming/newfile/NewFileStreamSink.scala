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

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.datasources.{FileFormat, FileFormatWriter}
import org.apache.spark.sql.execution.streaming.{FileStreamSink, FileStreamSinkLog, ManifestFileCommitProtocol, Sink}
import org.apache.spark.sql.{SparkSession, _}
import _root_.streaming.common.RenderEngine
import org.joda.time.DateTime

class NewFileStreamSink(
                         sparkSession: SparkSession,
                         _path: String,
                         fileFormat: FileFormat,
                         partitionColumnNames: Seq[String],
                         options: Map[String, String]) extends Sink with Logging {

  def evaluate(value: String, context: Map[String, AnyRef]) = {
    RenderEngine.render(value, context)
  }

  def path = {
    evaluate(_path, Map("pathDate" -> new DateTime()))
  }

  private def basePath = new Path(path)

  private def logPath = new Path(basePath, FileStreamSink.metadataDir)

  private def fileLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, logPath.toUri.toString)

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = path,
        isAppend = false)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ => // Do nothing
      }

      FileFormatWriter.write(
        sparkSession = sparkSession,
        queryExecution = data.queryExecution,
        fileFormat = fileFormat,
        committer = committer,
        outputSpec = FileFormatWriter.OutputSpec(path, Map.empty),
        hadoopConf = hadoopConf,
        partitionColumnNames = partitionColumnNames,
        bucketSpec = None,
        refreshFunction = _ => (),
        options = options)
    }
  }

  override def toString: String = s"FileSink[$path]"
}

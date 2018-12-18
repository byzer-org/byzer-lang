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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
  * Created by allwefantasy on 29/5/2018.
  */
class NewFileStreamSinkLog(
                            metadataLogVersion: Int,
                            sparkSession: SparkSession,
                            path: String
                          )
  extends CompactibleFileStreamLog[SinkFileStatus](metadataLogVersion, sparkSession, path) {

  private implicit val formats = Serialization.formats(NoTypeHints)

  protected override val fileCleanupDelayMs = sparkSession.sessionState.conf.fileSinkLogCleanupDelay

  protected override val isDeletingExpiredLog = sparkSession.sessionState.conf.fileSinkLogDeletion

  protected override val defaultCompactInterval =
    sparkSession.sessionState.conf.fileSinkLogCompactInterval

  require(defaultCompactInterval > 0,
    s"Please set ${SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key} (was $defaultCompactInterval) " +
      "to a positive value.")

  override def compactLogs(logs: Seq[SinkFileStatus]): Seq[SinkFileStatus] = {
    val deletedFiles = logs.filter(_.action == FileStreamSinkLog.DELETE_ACTION).map(_.path).toSet
    if (deletedFiles.isEmpty) {
      logs
    } else {
      logs.filter(f => !deletedFiles.contains(f.path))
    }
  }
}

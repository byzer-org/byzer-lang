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

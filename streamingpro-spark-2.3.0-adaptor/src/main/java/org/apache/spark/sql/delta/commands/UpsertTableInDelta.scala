package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta._
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

case class UpsertTableInDelta(_data: Dataset[_],
                              saveMode: Option[SaveMode],
                              outputMode: Option[OutputMode],
                              deltaLog: DeltaLog,
                              options: DeltaOptions,
                              partitionColumns: Seq[String],
                              configuration: Map[String, String]
                             ) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq()
  }
}

object UpsertTableInDelta {
  val ID_COLS = "idCols"
  val BATCH_ID = "batchId"
  val FILE_NAME = "__fileName__"
  val OPERATION_TYPE = "operation"
  val OPERATION_TYPE_UPSERT = "upsert"
  val OPERATION_TYPE_DELETE = "delete"
}



package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, AddFile, SetTransaction}
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession, functions => F}

case class UpsertTableInDelta(_data: Dataset[_],
                              saveMode: Option[SaveMode],
                              outputMode: Option[OutputMode],
                              deltaLog: DeltaLog,
                              options: DeltaOptions,
                              partitionColumns: Seq[String],
                              configuration: Map[String, String]
                             ) extends RunnableCommand
  with ImplicitMetadataOperation
  with DeltaCommand with DeltaCommandsFun {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(configuration.contains(UpsertTableInDelta.ID_COLS), "idCols is required ")

    if (outputMode.isDefined) {
      assert(outputMode.get == OutputMode.Append(), "append is required ")
    }

    if (saveMode.isDefined) {
      assert(saveMode.get == SaveMode.Append, "append is required ")
    }


    var actions = Seq[Action]()


    saveMode match {
      case Some(mode) =>
        deltaLog.withNewTransaction { txn =>
          actions = upsert(txn, sparkSession)
          val operation = DeltaOperations.Write(SaveMode.Overwrite,
            Option(partitionColumns),
            options.replaceWhere)
          txn.commit(actions, operation)
        }
      case None => outputMode match {
        case Some(mode) =>
          val queryId = sparkSession.sparkContext.getLocalProperty(StreamExecution.QUERY_ID_KEY)
          assert(queryId != null)

          if (SchemaUtils.typeExistsRecursively(_data.schema)(_.isInstanceOf[NullType])) {
            throw DeltaErrors.streamWriteNullTypeException
          }

          val txn = deltaLog.startTransaction()
          // Streaming sinks can't blindly overwrite schema.
          // See Schema Management design doc for details
          updateMetadata(
            txn,
            _data,
            partitionColumns,
            configuration = Map.empty,
            false)

          val currentVersion = txn.txnVersion(queryId)
          val batchId = configuration(UpsertTableInDelta.BATCH_ID).toLong
          if (currentVersion >= batchId) {
            logInfo(s"Skipping already complete epoch $batchId, in query $queryId")
          } else {
            actions = upsert(txn, sparkSession)
            val setTxn = SetTransaction(queryId,
              batchId, Some(deltaLog.clock.getTimeMillis())) :: Nil
            val info = DeltaOperations.StreamingUpdate(outputMode.get, queryId, batchId)
            txn.commit(setTxn ++ actions, info)
          }
      }
    }

    if (actions.size == 0) Seq[Row]() else {
      actions.map(f => Row.fromSeq(Seq(f.json)))
    }
  }

  def upsert(txn: OptimisticTransaction, sparkSession: SparkSession): Seq[Action] = {

    // if _data is stream dataframe, we should convert it to normal
    // dataframe and so we can join it later
    val data = if (_data.isStreaming) {
      class ConvertStreamDataFrame[T](encoder: ExpressionEncoder[T]) {

        def toBatch(data: Dataset[_]): Dataset[_] = {
          val resolvedEncoder = encoder.resolveAndBind(
            data.logicalPlan.output,
            data.sparkSession.sessionState.analyzer)
          val rdd = data.queryExecution.toRdd.map(resolvedEncoder.fromRow)(encoder.clsTag)
          val ds = data.sparkSession.createDataset(rdd)(encoder)
          ds
        }
      }
      new ConvertStreamDataFrame[Row](_data.asInstanceOf[Dataset[Row]].exprEnc).toBatch(_data)
    } else _data

    import sparkSession.implicits._
    val snapshot = deltaLog.snapshot
    val metadata = deltaLog.snapshot.metadata

    /**
      * Firstly, we should get all partition columns from `idCols` condition.
      * Then we can use them to optimize file scan.
      */
    val idCols = configuration.getOrElse(UpsertTableInDelta.ID_COLS, "")
    val idColsList = idCols.split(",").filterNot(_.isEmpty).toSeq
    val partitionColumnsInIdCols = partitionColumns.intersect(idColsList)


    val partitionFilters = if (partitionColumnsInIdCols.size > 0) {
      val schema = data.schema

      def isNumber(column: String) = {
        schema.filter(f => f.name == column).head.dataType match {
          case _: LongType => true
          case _: IntegerType => true
          case _: ShortType => true
          case _: DoubleType => true
          case _ => false
        }
      }

      val minMaxColumns = partitionColumnsInIdCols.flatMap { column =>
        Seq(F.lit(column), F.min(column).as(s"${column}_min"), F.max(F.max(s"${column}_max")))
      }.toArray
      val minxMaxKeyValues = data.select(minMaxColumns: _*).collect()

      // build our where statement
      val whereStatement = minxMaxKeyValues.map { row =>
        val column = row.getString(0)
        val minValue = row.get(1).toString
        val maxValue = row.get(2).toString

        if (isNumber(column)) {
          s"${column} >= ${minValue} and   ${maxValue} >= ${column}"
        } else {
          s"""${column} >= "${minValue}" and   "${maxValue}" >= ${column}"""
        }
      }
      logInfo(s"whereStatement: ${whereStatement.mkString(" and ")}")
      val predicates = parsePartitionPredicates(sparkSession, whereStatement.mkString(" and "))
      Some(predicates)

    } else None


    val filterFilesDataSet = partitionFilters match {
      case None =>
        snapshot.allFiles
      case Some(predicates) =>
        DeltaLog.filterFileList(
          metadata.partitionColumns, snapshot.allFiles.toDF(), predicates).as[AddFile]
    }

    // Again, we collect all files to driver,
    // this may impact performance and even make the driver OOM when
    // the number of files are very huge.
    // So please make sure you have configured the partition columns or make compaction frequently

    val filterFiles = filterFilesDataSet.collect
    val dataInTableWeShouldProcess = deltaLog.createDataFrame(snapshot, filterFiles, false)

    val dataInTableWeShouldProcessWithFileName = dataInTableWeShouldProcess.
      withColumn(UpsertTableInDelta.FILE_NAME, F.input_file_name())


    // get all files that are affected by the new data(update)
    val filesAreAffected = dataInTableWeShouldProcessWithFileName.join(data,
      usingColumns = idColsList,
      joinType = "inner").select(UpsertTableInDelta.FILE_NAME).
      distinct().collect().map(f => f.getString(0))

    val tmpFilePathSet = filesAreAffected.map(f => f.split("/").last).toSet

    val filesAreAffectedWithDeltaFormat = filterFiles.filter { file =>
      tmpFilePathSet.contains(file.path.split("/").last)
    }

    val deletedFiles = filesAreAffectedWithDeltaFormat.map(_.remove)

    // we should get  not changed records in affected files and write them back again
    val affectedRecords = deltaLog.createDataFrame(snapshot, filesAreAffectedWithDeltaFormat, false)

    val notChangedRecords = affectedRecords.join(data,
      usingColumns = idColsList, joinType = "leftanti").
      drop(F.col(UpsertTableInDelta.FILE_NAME))

    val notChangedRecordsNewFiles = txn.writeFiles(notChangedRecords, Some(options))
    val newFiles = txn.writeFiles(data, Some(options))

    notChangedRecordsNewFiles ++ newFiles ++ deletedFiles
  }

  override protected val canMergeSchema: Boolean = false
  override protected val canOverwriteSchema: Boolean = false

}

object UpsertTableInDelta {
  val ID_COLS = "idCols"
  val BATCH_ID = "batchId"
  val FILE_NAME = "__fileName__"
}



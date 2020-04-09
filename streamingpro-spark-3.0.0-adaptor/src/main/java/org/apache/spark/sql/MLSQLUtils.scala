package org.apache.spark.sql

import java.lang.reflect.Type

import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExplainMode, ExtendedMode}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedFunction}
import org.apache.spark.sql.types.DataType
import org.apache.spark.status.api.v1
import org.apache.spark.util.Utils

object MLSQLUtils {
  def getJavaDataType(tpe: Type): (DataType, Boolean) = {
    JavaTypeInference.inferDataType(tpe)
  }

  def getContextOrSparkClassLoader(): ClassLoader = {
    Utils.getContextOrSparkClassLoader
  }

  def localCanonicalHostName = {
    Utils.localCanonicalHostName()
  }

  def getAppStatusStore(sparkSession: SparkSession) = {
    sparkSession.sparkContext.statusStore
  }

  def createStage(stageId: Int) = {
    new v1.StageData(
      status = v1.StageStatus.PENDING,
      stageId = stageId,
      attemptId = 0,
      numTasks = 0,
      numActiveTasks = 0,
      numCompleteTasks = 0,
      numFailedTasks = 0,
      numKilledTasks = 0,
      numCompletedIndices = 0,

      submissionTime = None,
      firstTaskLaunchedTime = None,
      completionTime = None,
      failureReason = None,

      executorDeserializeTime = 0L,
      executorDeserializeCpuTime = 0L,
      executorRunTime = 0L,
      executorCpuTime = 0L,
      resultSize = 0L,
      jvmGcTime = 0L,
      resultSerializationTime = 0L,
      memoryBytesSpilled = 0L,
      diskBytesSpilled = 0L,
      peakExecutionMemory = 0L,
      inputBytes = 0L,
      inputRecords = 0L,
      outputBytes = 0L,
      outputRecords = 0L,
      shuffleRemoteBlocksFetched = 0L,
      shuffleLocalBlocksFetched = 0L,
      shuffleFetchWaitTime = 0L,
      shuffleRemoteBytesRead = 0L,
      shuffleRemoteBytesReadToDisk = 0L,
      shuffleLocalBytesRead = 0L,
      shuffleReadBytes = 0L,
      shuffleReadRecords = 0L,
      shuffleWriteBytes = 0L,
      shuffleWriteTime = 0L,
      shuffleWriteRecords = 0L,

      name = "Unknown",
      description = None,
      details = "Unknown",
      schedulingPool = null,

      rddIds = Nil,
      accumulatorUpdates = Nil,
      tasks = None,
      executorSummary = None,
      killedTasksSummary = Map())

  }

  def createExplainCommand(lg: LogicalPlan, extended: Boolean) = {
    ExplainCommand(lg, ExplainMode.fromString(ExtendedMode.name))
  }

  def createUserDefinedFunction(f: AnyRef,
                                dataType: DataType,
                                inputTypes: Option[Seq[DataType]]): UserDefinedFunction = {
    SparkUserDefinedFunction(f, dataType, Nil)
  }

}

package tech.mlsql.render.protocal

/**
 * 2019-01-28 WilliamZhu(allwefantasy@gmail.com)
 */
case class MLSQLScriptJobGroup(groupId: String,
                               activeJobsNum: Int,
                               completedJobsNum: Int,
                               failedJobsNum: Int,
                               activeJobs: Seq[MLSQLScriptJob]
                              )

case class MLSQLScriptJob(
                           val jobId: Int,
                           val submissionTime: Option[java.sql.Date],
                           val completionTime: Option[java.sql.Date],
                           val numTasks: Int,
                           val numActiveTasks: Int,
                           val numCompletedTasks: Int,
                           val numSkippedTasks: Int,
                           val numFailedTasks: Int,
                           val numKilledTasks: Int,
                           val numCompletedIndices: Int,
                           val numActiveStages: Int,
                           val numCompletedStages: Int,
                           val numSkippedStages: Int,
                           val numFailedStages: Int,
                           val duration: Long
                         )

case class MLSQLResourceRender(
                                currentJobGroupActiveTasks: Int,
                                activeTasks: Int,
                                failedTasks: Int,
                                completedTasks: Int,
                                totalTasks: Int,
                                taskTime: Double,
                                gcTime: Double,
                                activeExecutorNum: Int,
                                totalExecutorNum: Int,
                                totalCores: Int,
                                usedMemory: Double,
                                totalMemory: Double,
                                shuffleData: MLSQLShufflePerfRender
                              )

case class MLSQLShufflePerfRender(
                                   memoryBytesSpilled: Long,
                                   diskBytesSpilled: Long,
                                   inputRecords: Long

                                 )

package tech.mlsql.scheduler.client


/**
 * 27/2/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object SchedulerUtils {
  val DELTA_FORMAT = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"
  val SCHEDULER_DEPENDENCY_JOBS = "scheduler.dependency_jobs"
  val SCHEDULER_TIME_JOBS = "scheduler.time_jobs"
  val SCHEDULER_TIME_JOBS_STATUS = "scheduler.time_jobs_status"
  val SCHEDULER_LOG = "scheduler.log"

}

package tech.mlsql.scheduler.client

import it.sauronsoftware.cron4j._
import org.apache.spark.sql.SparkSession
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.ets.SchedulerCommand
import tech.mlsql.scheduler.algorithm.DAG
import tech.mlsql.scheduler.{CronOp, DependencyJob, JobNode, TimerJob}

/**
  * 2019-09-06 WilliamZhu(allwefantasy@gmail.com)
  */
class SchedulerTaskStore(spark: SparkSession, consoleUrl: String, consoleToken: String) extends TaskCollector with Logging with WowLog {

  import SchedulerCommand._
  import spark.implicits._

  override def getTasks: TaskTable = {
    val df = readTable(spark, SCHEDULER_TIME_JOBS)
    val taskTable = new TaskTable()
    val timer_data = df.as[TimerJob[Int]].collect()
    val dependency_data = try {
      readTable(spark, SCHEDULER_DEPENDENCY_JOBS).as[DependencyJob[Int]].collect()
    } catch {
      case e: Exception =>
        Array[DependencyJob[Int]]()
    }
    val dagInstance = DAG.build(dependency_data.map(f => (f.id, Option(f.dependency))).toSeq)
    
    timer_data.foreach { timerJob =>
      val schedulingPattern = new SchedulingPattern(timerJob.cron)

      def runTask = {
        import spark.implicits._
        val client = new MLSQLSchedulerClient[Int](consoleUrl, timerJob.owner, consoleToken)
        val leafIds = try {
          readTable(spark, SCHEDULER_TIME_JOBS_STATUS).as[JobNode[Int]].collect().toSeq
        } catch {
          case e: Exception =>
            dagInstance.findLeafNodeInTheSameTree(timerJob.id)
        }
        leafIds.filter(f => f.id == timerJob.id).headOption match {
          case Some(wow) =>
            logInfo(format(s"Execute cron job ${timerJob.id}. With dependency."))
            client.execute(wow)
            // if all jobs success, trigger the dependency jobs.
            if (leafIds.filter(f => f.isLastSuccess).size == leafIds.size) {
              dagInstance.topologicalSort.reverse.foreach { job =>
                if (leafIds.filter(f => f.id == job.id).size == 0) {
                  logInfo(format(s"Execute dependency job ${job.id}. trigger job ${timerJob.id}"))
                  client.execute(job)
                }
              }
              leafIds.foreach(f => f.cleanStatus)
            }
          case None =>
            logInfo(format(s"Execute cron job ${timerJob.id}. No dependency."))
            client.execute(JobNode(timerJob.id, 0, 0, Seq(), Seq(), Seq(), Option(CronOp(timerJob.cron)), timerJob.owner))
        }

        if (leafIds.size > 0) {
          saveTable(spark, spark.createDataset[JobNode[Int]](leafIds.toSeq).toDF(), SCHEDULER_TIME_JOBS_STATUS)
        }

      }

      taskTable.add(schedulingPattern, new Task() {
        override def execute(context: TaskExecutionContext): Unit = {
          try {
            runTask
          } catch {
            case e: Exception =>
              //design how to  handle the exception
              e.printStackTrace()
          }
        }
      })
    }
    taskTable
  }
}

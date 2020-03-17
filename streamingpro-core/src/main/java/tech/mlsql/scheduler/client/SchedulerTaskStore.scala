package tech.mlsql.scheduler.client

import it.sauronsoftware.cron4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.scheduler.algorithm.DAG
import tech.mlsql.scheduler.{CronOp, DependencyJob, JobNode, TimerJob}
import tech.mlsql.store.DBStore
import org.apache.spark.sql.{functions => F}

/**
  * 2019-09-06 WilliamZhu(allwefantasy@gmail.com)
  */
class SchedulerTaskStore(spark: SparkSession, _consoleUrl: String, _consoleToken: String) extends TaskCollector with Logging with WowLog {

  import SchedulerUtils._
  import spark.implicits._

  override def getTasks: TaskTable = {
    val timer_data = try {
      DBStore.store.readTable(spark, SCHEDULER_TIME_JOBS).withColumn("id",F.col("id").cast(IntegerType)).as[TimerJob[Int]].collect()
    } catch {
      case e: Exception =>
        logWarning(s"${SCHEDULER_TIME_JOBS} not created before visit", e)
        Array[TimerJob[Int]]()
    }
    val taskTable = new TaskTable()
    val dependency_data = try {
      DBStore.store.readTable(spark, SCHEDULER_DEPENDENCY_JOBS).withColumn("id",F.col("id").cast(IntegerType))
        .withColumn("dependency",F.col("dependency").cast(IntegerType)).as[DependencyJob[Int]].collect()
    } catch {
      case e: Exception =>
        Array[DependencyJob[Int]]()
    }
    val dagInstance = DAG.build(dependency_data.map(f => (f.id, Option(f.dependency))).toSeq)
    val executeItems = dagInstance.topologicalSort.reverse
    val consoleToken = _consoleToken
    val consoleUrl = _consoleUrl

    timer_data.foreach { timerJob =>
      val schedulingPattern = new SchedulingPattern(timerJob.cron)
      val nodes = dagInstance.findNodeInTheSameTree(timerJob.id)
      val recomputeLeafIds = dagInstance.findLeafNodeInTheSameTree(timerJob.id)

      def runTask = {
        import spark.implicits._

        val leafIds = try {
          DBStore.store.readTable(spark, SCHEDULER_TIME_JOBS_STATUS).as[JobNode[Int]].collect().toSeq
        } catch {
          case e: Exception =>
            recomputeLeafIds
        }

        def runInSpark(jobNode: JobNode[Int]) = {
          spark.sparkContext.setJobGroup(s"scheduler-job-${jobNode.owner}-${jobNode.id}", s"trigger from: ${timerJob.owner}-${timerJob.id}:", true)
          val newow = spark.sparkContext.parallelize(Seq(""), 1).map { item =>
            val client = new MLSQLSchedulerClient[Int](consoleUrl, timerJob.owner, consoleToken)
            client.execute(jobNode)
            jobNode
          }.collect.head
          //sync status
          jobNode.isSuccess = newow.isSuccess
          jobNode.isExecuted = newow.isExecuted
          jobNode.msg = newow.msg
        }

        leafIds.filter(f => f.id == timerJob.id).headOption match {
          case Some(wow) =>
            val startTime = System.currentTimeMillis()
            runInSpark(wow)
            val logItem = SchedulerLogItem(startTime, System.currentTimeMillis(), wow, wow, nodes.toSeq)
            DBStore.store.saveTable(spark, spark.createDataset(Seq(logItem)).toDF(), SCHEDULER_LOG, None, false)

            // if all jobs success, trigger the dependency jobs.
            if (leafIds.filter(f => f.isLastSuccess).size == leafIds.size) {
              // subtree 

              logItem.dependencies = nodes.toSeq
              val ids = nodes.map(f => f.id).toSet
              executeItems.filter(f => ids.contains(f.id)).foreach { job =>
                if (leafIds.filter(f => f.id == job.id).size == 0) {
                  val startTime = System.currentTimeMillis()
                  runInSpark(job)
                  val logItem = SchedulerLogItem(startTime, System.currentTimeMillis(), job, wow, dagInstance.findNodeInTheSameTree(job.id).toSeq)
                  DBStore.store.saveTable(spark, spark.createDataset(Seq(logItem)).toDF(), SCHEDULER_LOG, None, false)
                }
              }
              leafIds.foreach(f => f.cleanStatus)
            }
          case None =>
            val startTime = System.currentTimeMillis()
            val jobNode = JobNode(timerJob.id, 0, 0, Seq(), Seq(), Seq(), Option(CronOp(timerJob.cron)), timerJob.owner)
            runInSpark(jobNode)
            val logItem = SchedulerLogItem(startTime, System.currentTimeMillis(), jobNode, jobNode, nodes.toSeq)
            DBStore.store.saveTable(spark, spark.createDataset(Seq(logItem)).toDF(), SCHEDULER_LOG, None, false)
        }

        if (leafIds.size > 0) {
          DBStore.store.saveTable(spark, spark.createDataset[JobNode[Int]](leafIds.toSeq).toDF(), SCHEDULER_TIME_JOBS_STATUS, Option("id"), false)
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

case class SchedulerLogItem(var startTime: Long, var endTime: Long, var currentRun: JobNode[Int], var timerJob: JobNode[Int], var dependencies: Seq[JobNode[Int]])

package streaming.core.datasource.util

import org.apache.spark.MLSQLResource
import org.apache.spark.sql.SparkSession
import tech.mlsql.job.JobManager


/**
 * 2019-01-22 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLJobCollect(spark: SparkSession, owner: String) {
  val resource = new MLSQLResource(spark, owner, getGroupId)

  def jobs = {
    val infoMap = JobManager.getJobInfo
    val data = infoMap.toSeq.map(_._2).filter(_.owner == owner)
    data
  }

  def getJob(jobName: String) = {
    val infoMap = JobManager.getJobInfo
    val data = infoMap.toSeq.map(_._2).filter(_.owner == owner).filter(_.groupId == getGroupId(jobName))
    data
  }

  def getGroupId(jobNameOrGroupId: String) = {
    JobManager.getJobInfo.filter(f => f._2.jobName == jobNameOrGroupId).headOption match {
      case Some(item) => item._2.groupId
      case None => jobNameOrGroupId
    }
  }

  def resourceSummary(jobGroupId: String) = {
    resource.resourceSummary(jobGroupId)
  }


  def jobDetail(jobGroupId: String, version: Int = 1) = {
    if (version == 1) {
      resource.jobDetail(jobGroupId)
    }
    else {
      resource.jobDetail2(jobGroupId)
    }

  }

  def jobProgress(jobGroupId: String) = {
    val finalJobGroupId = getGroupId(jobGroupId)
    val stream = spark.streams.get(finalJobGroupId)
    if (stream != null) {
      stream.recentProgress.map { f =>
        f.json
      }
    } else {
      Array[String]()
    }
  }
}

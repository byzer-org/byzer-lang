package streaming.core.datasource.util

import org.apache.spark.MLSQLResource
import org.apache.spark.sql.SparkSession
import streaming.core.StreamingproJobManager


/**
  * 2019-01-22 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLJobCollect(spark: SparkSession, owner: String) {
  val resource = new MLSQLResource(spark, owner, getGroupId)
  
  def jobs = {
    val infoMap = StreamingproJobManager.getJobInfo
    val data = infoMap.toSeq.map(_._2).filter(_.owner == owner)
    data
  }

  def getGroupId(jobNameOrGroupId: String) = {
    StreamingproJobManager.getJobInfo.filter(f => f._2.jobName == jobNameOrGroupId).headOption match {
      case Some(item) => item._2.groupId
      case None => jobNameOrGroupId
    }
  }

  def resourceSummary(jobGroupId: String) = {
    resource.resourceSummary(jobGroupId)
  }


  def jobDetail(jobGroupId: String) = {
    resource.jobDetail(jobGroupId)
  }
}

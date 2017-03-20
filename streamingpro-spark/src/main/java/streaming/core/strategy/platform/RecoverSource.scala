package streaming.core.strategy.platform

import org.apache.spark.streaming.Time

/**
 * 5/9/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait SparkStreamingRecoverSource {
  def saveJobSate(time: Time)

  def recoverPath: Option[String]

  def restoreJobSate(jobName: String): Unit

  def jobSate(time: Time):Map[String,Any]
}

trait RecoverSource

package org.apache.spark.kafka

import _root_.kafka.common.TopicAndPartition
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}

/**
 * 9/26/16 WilliamZhu(allwefantasy@gmail.com)
 */
object KafkaClusterService {

  def latestKafkaOffsetFromKafkaCluster(kafkaParams: Map[String, String], topics: Set[String]) = {
    val kc = new KafkaCluster(kafkaParams)
    val fromOffsets = getFromOffsets(kc, kafkaParams, topics)
    fromOffsets
  }

  def kafkaRange(fromOffsets: Map[TopicAndPartition, Long], toOffsets: Map[TopicAndPartition, Long]) = {
    fromOffsets.map { fo =>
      val toOffset = toOffsets.filter(to => to._1 == fo._1).head
      OffsetRange(fo._1, fo._2, toOffset._2)
    }.toArray
  }

  def restoreKafkaOffsetFromHDFS(context: SparkContext, pathDir: String, suffix: String): Map[TopicAndPartition, Long] = {

    val fileSystem = FileSystem.get(context.hadoopConfiguration)

    if (!fileSystem.exists(new Path(pathDir))) {
      return null
    }

    val files = FileSystem.get(context.hadoopConfiguration).listStatus(new Path(pathDir)).toList
    if (files.length == 0) {
      return null
    }

    val jobFiles = files.filter(f => f.getPath.getName.endsWith("_" + suffix)).sortBy(f => f.getPath.getName).reverse
    if (jobFiles.length == 0) return null

    val restoreKafkaFile = jobFiles.head.getPath.getName


    val lines = context.textFile(pathDir + "/" + restoreKafkaFile).map { f =>
      val Array(topic, partition, from) = f.split(",")
      (topic, partition.toInt, from.toLong)
    }.collect().groupBy(f => f._1)

    val fromOffsets = lines.flatMap { topicPartitions =>
      topicPartitions._2.map { f =>
        (TopicAndPartition(f._1, f._2) -> f._3)
      }
    }
    fromOffsets
  }

  private def getFromOffsets(
                              kc: KafkaCluster,
                              kafkaParams: Map[String, String],
                              topics: Set[String]
                              ): Map[TopicAndPartition, Long] = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
        leaderOffsets.map { case (tp, lo) =>
          (tp, lo.offset)
        }
      }
    KafkaCluster.checkErrors(result)
  }
}

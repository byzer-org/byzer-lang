package streaming.core.compositor.spark.hdfs

import java.text.SimpleDateFormat
import java.util.Date

import kafka.common.TopicAndPartition
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext

/**
 * 5/5/16 WilliamZhu(allwefantasy@gmail.com)
 */
object HDFSOperator {


  def saveKafkaOffset(context: StreamingContext, path: String, suffix: String, offsets: Any) = {

    def getTime(pattern: String): String = {
      new SimpleDateFormat(pattern).format(new Date())
    }

    val fileSystem = FileSystem.get(context.sparkContext.hadoopConfiguration)

    if (!fileSystem.exists(new Path(path))) {
      fileSystem.mkdirs(new Path(path))
    }

    val item = getTime("yyyyMMddHHmmss") + "_" + suffix
    val res = offsets.asInstanceOf[Map[TopicAndPartition, Long]].map { or =>
      s"${or._1.topic},${or._1.partition},${or._2}"
    }.map(f => ("", f))
    saveFile(path, item, res.toIterator)
  }



  def kafkaOffset(context: StreamingContext, pathDir: String, suffix: String) = {
    val files = FileSystem.get(context.sparkContext.hadoopConfiguration).listStatus(new Path(pathDir)).toList
    if (files.length == 0) {
      throw new SparkException(s"no upgradePath: $pathDir ")
    }

    val restoreKafkaFile = files.filter(f => f.getPath.getName.endsWith("_" + suffix)).
      sortBy(f => f.getPath.getName).reverse.head.getPath.getName

    val lines = context.sparkContext.textFile(pathDir + "/" + restoreKafkaFile).map { f =>
      val Array(topic, partition, from) = f.split(",")
      (topic, partition.toInt, from.toLong)
    }.collect().groupBy(f => f._1)

    val fromOffsets = lines.flatMap { topicPartitions =>
      topicPartitions._2.map { f =>
        (TopicAndPartition(f._1, f._2), f._3)
      }.toMap
    }
    fromOffsets
  }

  def saveFile(path: String, fileName: String, iterator: Iterator[(String, String)]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(path + s"/$fileName"), true)
      iterator.foreach { x =>
        dos.writeBytes(x._2 + "\n")
      }
    } catch {
      case ex: Exception =>
        println("file save exception")
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            println("close exception")
        }
        dos.close()
      }
    }

  }
}

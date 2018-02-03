package streaming.core

import java.net.URL

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Created by allwefantasy on 28/3/2017.
  */
object Test {
  var psDriverUrl: String = null
  var psExecutorId: String = null
  var hostname: String = null
  var cores: Int = 0
  var appId: String = null
  val psDriverPort = 7777
  var psDriverHost: String = null
  var workerUrl: Option[String] = None
  val userClassPath = new mutable.ListBuffer[URL]()

  def parseArgs = {
    //val runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    //var argv = runtimeMxBean.getInputArguments.toList
    var argv = "org.apache.spark.executor.CoarseGrainedExecutorBackend --driver-url spark://CoarseGrainedScheduler@192.168.219.49:64455 --executor-id 0 --hostname 192.168.219.49 --cores 1 --app-id app-20180202171521-0037 --worker-url spark://Worker@192.168.219.49:60980".split("\\s+").toList
    var count = 0
    var first = 0
    argv.foreach { f =>
      if (f.startsWith("--") && first == 0) {
        first = count
      }
      count += 1
    }
    argv = argv.drop(first)

    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          psDriverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          psExecutorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
      }
    }
    val Array(host, port) = psDriverUrl.split(":")
    psDriverHost = host
    psDriverUrl = psDriverHost + ":" + psDriverPort
  }

  def main(args: Array[String]): Unit = {
    parseArgs
    println(psDriverUrl)

  }
}

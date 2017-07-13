package streaming.db

import net.csdn.common.logging.Loggers

/**
  * Created by allwefantasy on 12/7/2017.
  */
object ManagerConfiguration {
  var yarnUrl = ""
  val liveness_check_interval = 30
  val clean_check_interval = 30
  val submit_progress_check_interval = 2
  val submit_progress_check_expire_duration = 60 * 10
  val resubmit_try_interval = 60
}

class SparkAppTool {
  val logger = Loggers.getLogger(classOf[SparkAppTool])


  val keywords = Set(
    "class",
    "master",
    "name",
    "executor-memory",
    "driver-memory",
    "num-executors",
    "executor-cores",
    "jars",
    "files")

  def process(params: Map[String, String]) = {
    val sourceK = params.filter(f => f._1.startsWith("mmspark.")).map(f => (cut(f._1), f._2)).map { f =>
      val paramKey = f._1
      val paramValue = f._2
      paramKey match {
        case pk if pk.startsWith("spark.") => s"""--conf "${paramKey}=${paramValue}" """
        case pk if keywords.contains(pk) => s"""--${paramKey} $paramValue """
        case pk if pk == "args" => s"""$paramValue"""
        case _ => s"""-${paramKey} $paramValue """
      }

    }

    val source = sourceK.filter(f => f.startsWith("--")).mkString(" ") + " " +
      s"""  ${params("jarPath")}""" + " " +
      sourceK.filter(f => f.startsWith("-") && !f.startsWith("--")).mkString(" ") + " " +
      sourceK.filter(f => !f.startsWith("-") && !f.startsWith("--")).mkString(" ")

    logger.info(source)
    TSparkApplication.save("", ManagerConfiguration.yarnUrl, "spark-submit " + source)
  }

  private def cut(str: String) = {
    val abc = str.split("\\.")
    abc.takeRight(abc.length - 1).mkString(".")
  }

  def name: String = "spark"
}



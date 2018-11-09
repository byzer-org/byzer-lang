package org.apache.spark

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{ShutdownHookManager, Utils, VersionUtils}
import org.slf4j.Logger

import scala.util.matching.Regex

/**
  * Created by allwefantasy on 1/6/2018.
  */
object MLSQLSparkConst {
  val SPARK_PREFIX = "spark."
  val YARN_PREFIX = "yarn."
  val HADOOP_PRFIX = "hadoop."
  val SPARK_HADOOP_PREFIX = SPARK_PREFIX + HADOOP_PRFIX
  val DRIVER_PREFIX = "driver."

  val KEYTAB = SPARK_PREFIX + YARN_PREFIX + "keytab"
  val PRINCIPAL = SPARK_PREFIX + YARN_PREFIX + "principal"
  val DRIVER_BIND_ADDR = SPARK_PREFIX + DRIVER_PREFIX + "bindAddress"

  val HIVE_VAR_PREFIX: Regex = """set:hivevar:([^=]+)""".r
  val USE_DB: Regex = """use:([^=]+)""".r
  val QUEUE = SPARK_PREFIX + YARN_PREFIX + "queue"
  val DEPRECATED_QUEUE = "mapred.job.queue.name"

  val SPARK_VERSION = org.apache.spark.SPARK_VERSION

  def addShutdownHook(f: () => Unit): Unit = {
    ShutdownHookManager.addShutdownHook(f)
  }

  def initDaemon(log: Logger): Unit = {
    Utils.initDaemon(log)
  }

  def getJobGroupIDKey(): String = SparkContext.SPARK_JOB_GROUP_ID

  def exceptionString(e: Throwable): String = {
    Utils.exceptionString(e)
  }

  def getCurrentUserName(): String = {
    Utils.getCurrentUserName()
  }

  def getContextOrSparkClassLoader(): ClassLoader = {
    Utils.getContextOrSparkClassLoader
  }

  def getLocalDir(conf: SparkConf): String = {
    Utils.getLocalDir(conf)
  }

  def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "spark"): File = {
    Utils.createTempDir(root, namePrefix)
  }

  def getUserJars(conf: SparkConf): Seq[String] = {
    Utils.getUserJars(conf)
  }

  def newConfiguration(conf: SparkConf): Configuration = {
    SparkHadoopUtil.get.newConfiguration(conf)
  }

  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit): Unit = {
    Utils.tryLogNonFatalError(block)
  }

  def localHostName(): String = Utils.localHostName()

  // org.apache.spark.util.VersionUtils: Utilities for working with Spark version strings

  def majorVersion(sparkVersion: String): Int = VersionUtils.majorVersion(sparkVersion)

  def minorVersion(sparkVersion: String): Int = VersionUtils.minorVersion(sparkVersion)

}

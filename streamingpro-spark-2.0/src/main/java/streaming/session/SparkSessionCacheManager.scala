package streaming.session

/**
  * Created by allwefantasy on 3/6/2018.
  */

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import streaming.log.Logging
import org.apache.spark.MLSQLConf._


class SparkSessionCacheManager(conf: SparkConf) extends Logging {
  private val cacheManager =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())

  private[this] val userToSparkSession =
    new ConcurrentHashMap[String, (SparkSession, AtomicInteger)]

  private[this] val userLatestLogout = new ConcurrentHashMap[String, Long]
  private[this] val idleTimeout =
    math.max(conf.getTimeAsMs(SESSION_IDLE_TIMEOUT.key), 60 * 1000)

  def set(user: String, sparkSession: SparkSession): Unit = {
    userToSparkSession.put(user, (sparkSession, new AtomicInteger(1)))
  }

  def getAndIncrease(user: String): Option[SparkSession] = {
    Some(userToSparkSession.get(user)) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        log.info(s"SparkSession for [$user] is reused for ${times.incrementAndGet()} times.")
        Some(ss)
      case _ =>
        log.info(s"SparkSession for [$user] isn't cached, will create a new one.")
        None
    }
  }

  def decrease(user: String): Unit = {
    Some(userToSparkSession.get(user)) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        userLatestLogout.put(user, System.currentTimeMillis())
        log.info(s"SparkSession for [$user] is reused for ${times.decrementAndGet()} times.")
      case _ =>
        log.warn(s"SparkSession for [$user] was not found in the cache.")
    }
  }

  private[this] def removeSparkSession(user: String): Unit = {
    userToSparkSession.remove(user)
  }

  private[this] val sessionCleaner = new Runnable {
    override def run(): Unit = {
      userToSparkSession.asScala.foreach {
        case (user, (session, _)) if session.sparkContext.isStopped =>
          log.warn(s"SparkSession for $user might already be stopped by forces outside ," +
            s" cleaning it..")
          removeSparkSession(user)
        case (user, (_, times)) if times.get() > 0 =>
          log.debug(s"There are $times active connection(s) bound to the SparkSession instance" +
            s" of $user ")
        case (user, (_, _)) if !userLatestLogout.containsKey(user) =>
        case (user, (session, _))
          if userLatestLogout.get(user) + idleTimeout <= System.currentTimeMillis() =>
          log.info(s"Stopping idle SparkSession for user [$user].")
          removeSparkSession(user)
          session.stop()
          if (conf.get("spark.master").startsWith("yarn")) {
            System.setProperty("SPARK_YARN_MODE", "true")
          }
        case _ =>
      }
    }
  }

  /**
    * Periodically close idle SparkSessions in 'spark.kyuubi.session.clean.interval(default 5min)'
    */
  def start(): Unit = {
    // at least 1 minutes
    val interval = math.max(conf.getTimeAsSeconds(SESSION_CHECK_INTERVAL.key), 60)
    log.info(s"Scheduling SparkSession cache cleaning every $interval seconds")
    cacheManager.scheduleAtFixedRate(sessionCleaner, interval, interval, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    log.info("Stopping SparkSession Cache Manager")
    cacheManager.shutdown()
    userToSparkSession.asScala.values.foreach { kv => kv._1.stop() }
    userToSparkSession.clear()
  }
}

object SparkSessionCacheManager {

  type Usage = AtomicInteger

  private[this] var sparkSessionCacheManager: SparkSessionCacheManager = _

  def startCacheManager(conf: SparkConf): Unit = {
    sparkSessionCacheManager = new SparkSessionCacheManager(conf)
    sparkSessionCacheManager.start()
  }

  def get: SparkSessionCacheManager = sparkSessionCacheManager
}

package streaming.session

import java.lang.reflect.{UndeclaredThrowableException}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{TimeoutException}

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import streaming.log.Logging

import scala.collection.mutable.{HashSet => MHSet}


import org.apache.spark.MLSQLConf._
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}


/**
  * Created by allwefantasy on 3/6/2018.
  */
class MLSQLSparkSession(userName: String, conf: Map[String, String]) extends Logging {
  private[this] var _sparkSession: SparkSession = _

  def sparkSession: SparkSession = _sparkSession


  private[this] def getOrCreate(sessionConf: Map[String, String]): Unit = synchronized {
    var checkRound = 15
    val interval = 1l
    // if user's sc is being constructed by another
    while (MLSQLSparkSession.isPartiallyConstructed(userName)) {
      wait(interval)
      checkRound -= 1
      if (checkRound <= 0) {
        throw new MLSQLException(s"A partially constructed SparkContext for [$userName] " +
          s"has last more than ${checkRound * interval} seconds")
      }
      log.info(s"A partially constructed SparkContext for [$userName], $checkRound times countdown.")
    }

    SparkSessionCacheManager.get.getAndIncrease(userName) match {
      case Some(ss) =>
        _sparkSession = ss.newSession()
      case _ =>
        MLSQLSparkSession.setPartiallyConstructed(userName)
        notifyAll()
        create(sessionConf)
    }
  }


  private[this] def create(sessionConf: Map[String, String]): Unit = {
    log.info(s"--------- Create new SparkSession for $userName ----------")
    try {
      _sparkSession = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession.newSession()
      SparkSessionCacheManager.get.set(userName, _sparkSession)
    } catch {
      case e: Exception =>
        throw new MLSQLException(
          s"Get SparkSession for [$userName] failed: " + e, "", e)
    } finally {
      MLSQLSparkSession.setFullyConstructed(userName)
    }
  }

  def isLocalMaster(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master == "local" || master.startsWith("local[")
  }


  def init(sessionConf: Map[String, String]): Unit = {
    try {
      getOrCreate(sessionConf)
    } catch {
      case ute: UndeclaredThrowableException => throw ute.getCause
      case e: Exception => throw e
    }
  }
}

object MLSQLSparkSession extends Logging {
  private[this] val userSparkContextBeingConstruct = new MHSet[String]()

  def setPartiallyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.add(user)
  }

  def isPartiallyConstructed(user: String): Boolean = {
    userSparkContextBeingConstruct.contains(user)
  }

  def setFullyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.remove(user)
  }

}

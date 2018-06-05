package streaming.session

import java.lang.reflect.{Modifier, UndeclaredThrowableException}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{TimeUnit, TimeoutException}

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{MLSQLSparkConst, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import streaming.log.Logging

import scala.collection.mutable.{HashSet => MHSet}
import scala.concurrent.{Await, Promise}
import scala.util.control.NonFatal
import org.apache.spark.MLSQLConf._
import org.apache.spark.MLSQLSparkConst._
import streaming.core.strategy.platform.MLSQLRuntime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * Created by allwefantasy on 3/6/2018.
  */
class SparkSessionWithUGI(user: UserGroupInformation, conf: SparkConf) extends Logging {
  private[this] var _sparkSession: SparkSession = _

  def sparkSession: SparkSession = _sparkSession

  private[this] val promisedSparkContext = Promise[SparkContext]()
  private[this] var initialDatabase: Option[String] = None

  private[this] val sparkContext: AtomicReference[SparkContext] = new AtomicReference[SparkContext]()

  private[this] def newContext(): Thread = {
    new Thread(s"Start-SparkContext-$userName") {
      override def run(): Unit = {
        try {
          promisedSparkContext.trySuccess(new SparkContext(conf))
        } catch {
          case NonFatal(e) =>
            throw e
        }
      }
    }
  }

  /**
    * Setting configuration from connection strings for existing SparkSession
    *
    * @param sessionConf configurations for user connection string
    */
  private[this] def configureSparkSession(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            _sparkSession.conf.set(k, value)
          } else {
            _sparkSession.conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case "use:database" => initialDatabase = Some("use " + value)
        case _ =>
      }
    }
  }

  private[this] def configureSparkConf(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(DEPRECATED_QUEUE) => conf.set(QUEUE, value)
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            conf.set(k, value)
          } else {
            conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case "use:database" => initialDatabase = Some("use " + value)
        case _ =>
      }
    }

    // proxy user does not have rights to get token as real user
    conf.remove(KEYTAB)
    conf.remove(PRINCIPAL)
  }

  private[this] def getOrCreate(sessionConf: Map[String, String], params: Map[Any, Any]): Unit = synchronized {
    var checkRound = math.max(conf.get(SESSION_WAIT_OTHER_TIMES.key).toInt, 15)
    val interval = conf.getTimeAsMs(SESSION_WAIT_OTHER_INTERVAL.key)
    // if user's sc is being constructed by another
    while (SparkSessionWithUGI.isPartiallyConstructed(userName)) {
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
        configureSparkSession(sessionConf)
      case _ =>
        SparkSessionWithUGI.setPartiallyConstructed(userName)
        notifyAll()
        create(sessionConf, params)
    }
  }

  private[this] def configureUDF = {
    def registerUDF(clzz: String) = {
      log.info("register functions.....")
      Class.forName(clzz).getMethods.foreach { f =>
        try {
          if (Modifier.isStatic(f.getModifiers)) {
            log.info(f.getName)
            f.invoke(null, sparkSession.udf)
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }

    }
    List(
      "streaming.core.compositor.spark.udf.Functions",
      "streaming.crawler.udf.Functions",
      "streaming.dsl.mmlib.algs.processing.UDFFunctions").foreach { clzz =>
      registerUDF(clzz)
    }


  }

  private[this] def create(sessionConf: Map[String, String], params: Map[Any, Any]): Unit = {
    log.info(s"--------- Create new SparkSession for $userName ----------")
    val appName = s"MLSQLSession[$userName]@" + conf.get(BIND_HOST.key)
    conf.setAppName(appName)
    configureSparkConf(sessionConf)

    val allowMultipleContexts: Boolean =
      conf.getBoolean("spark.driver.allowMultipleContexts", false)
    val totalWaitTime: Long = conf.getTimeAsSeconds(SESSTION_INIT_TIMEOUT.key)

    synchronized {
      if (!allowMultipleContexts) {
        sparkContext.set(params("__sc__").asInstanceOf[SparkContext])
      }
    }

    try {
      user.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          if (allowMultipleContexts) {
            newContext().start()
            val context =
              Await.result(promisedSparkContext.future, Duration(totalWaitTime, TimeUnit.SECONDS))
            sparkContext.set(context)
          }
          val context = sparkContext.get()

          _sparkSession = ReflectUtils.newInstance(
            classOf[SparkSession].getName,
            Seq(classOf[SparkContext]),
            Seq(context)).asInstanceOf[SparkSession]
          configureUDF
        }
      })
      SparkSessionCacheManager.get.set(userName, _sparkSession)
    } catch {
      case ute: UndeclaredThrowableException =>
        ute.getCause match {
          case te: TimeoutException =>
            stopContext()
            throw new MLSQLException(
              s"Get SparkSession for [$userName] failed: " + te, "08S01", 1001, te)
          case _ =>
            stopContext()
            throw new MLSQLException(ute.toString, "08S01", ute.getCause)
        }
      case e: Exception =>
        stopContext()
        throw new MLSQLException(
          s"Get SparkSession for [$userName] failed: " + e, "08S01", e)
    } finally {
      SparkSessionWithUGI.setFullyConstructed(userName)
      newContext().join()
    }
  }

  def isLocalMaster(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master == "local" || master.startsWith("local[")
  }

  /**
    * Invoke SparkContext.stop() if not succeed initializing it
    */
  private[this] def stopContext(): Unit = {
    promisedSparkContext.future.map { sc =>
      log.warn(s"Error occurred during initializing SparkContext for $userName, stopping")
      sc.stop
      System.setProperty("SPARK_YARN_MODE", "true")
    }
  }

  def userName: String = user.getShortUserName

  def init(sessionConf: Map[String, String], params: Map[Any, Any]): Unit = {
    try {
      getOrCreate(sessionConf, params)
      initialDatabase.foreach { db =>
        user.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = _sparkSession.sql(db)
        })
      }
    } catch {
      case ute: UndeclaredThrowableException => throw ute.getCause
      case e: Exception => throw e
    }
  }
}

object SparkSessionWithUGI extends Logging {
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

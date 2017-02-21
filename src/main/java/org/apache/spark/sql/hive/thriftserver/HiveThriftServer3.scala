package org.apache.spark.sql.hive.thriftserver

import java.io.PrintStream
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.thrift.{ThriftBinaryCLIService, ThriftHttpCLIService}
import org.apache.hive.service.server.{HiveServer2, HiveServerServerOptionsProcessor}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobStart, StatsReportListener}
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.HiveThriftServer2Listener
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.{Logging, SparkContext}

/**
  * Created by allwefantasy on 20/2/2017.
  */
object HiveThriftServer3 extends Logging {
  var LOG = LogFactory.getLog(classOf[HiveServer2])
  var uiTab: Option[ThriftServerTab] = _
  var listener: HiveThriftServer2Listener = _

  /**
    * :: DeveloperApi ::
    * Starts a new thrift server with the given context.
    */
  @DeveloperApi
  def startWithContext(sqlContext: HiveContext): Unit = {
    val server = new HiveThriftServer3(sqlContext)
    server.init(sqlContext.hiveconf)
    server.start()
    listener = new HiveThriftServer2Listener(server, sqlContext.conf)
    sqlContext.sparkContext.addSparkListener(listener)
    uiTab = if (sqlContext.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
      Some(new ThriftServerTab(sqlContext.sparkContext))
    } else {
      None
    }
  }

  def run(hiveContext: HiveContext) {

    hiveContext.sparkContext.addSparkListener(new StatsReportListener())
    hiveContext.metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
    hiveContext.metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
    hiveContext.metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))
    hiveContext.setConf("spark.sql.hive.version", HiveContext.hiveExecutionVersion)

    SparkSQLEnv.sparkContext = hiveContext.sparkContext
    SparkSQLEnv.hiveContext = hiveContext

    ShutdownHookManager.addShutdownHook { () =>
      SparkSQLEnv.stop()
      uiTab.foreach(_.detach())
    }

    try {
      val server = new HiveThriftServer3(SparkSQLEnv.hiveContext)
      server.init(SparkSQLEnv.hiveContext.hiveconf)
      server.start()
      logInfo("HiveThriftServer2 started:"+hiveContext.getClass)
      listener = new HiveThriftServer2Listener(server, SparkSQLEnv.hiveContext.conf)
      HiveThriftServer2.listener = listener
      SparkSQLEnv.sparkContext.addSparkListener(listener)
      uiTab = if (SparkSQLEnv.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
        Some(new ThriftServerTab(SparkSQLEnv.sparkContext))
      } else {
        None
      }
      // If application was killed before HiveThriftServer2 start successfully then SparkSubmit
      // process can not exit, so check whether if SparkContext was stopped.
      if (SparkSQLEnv.sparkContext.stopped.get()) {
        logError("SparkContext has stopped even if HiveServer2 has started, so exit")
        System.exit(-1)
      }
    } catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    }
  }

  private[hive] class HiveThriftServer3(hiveContext: HiveContext)
    extends HiveServer2
      with ReflectedCompositeService {
    // state is tracked internally so that the server only attempts to shut down if it successfully
    // started, and then once only.
    private val started = new AtomicBoolean(false)

    override def init(hiveConf: HiveConf) {
      val sparkSqlCliService = new SparkSQLCLIService(this, hiveContext)
      setSuperField(this, "cliService", sparkSqlCliService)
      addService(sparkSqlCliService)

      val thriftCliService = if (isHTTPTransportMode(hiveConf)) {
        new ThriftHttpCLIService(sparkSqlCliService)
      } else {
        new ThriftBinaryCLIService(sparkSqlCliService)
      }

      setSuperField(this, "thriftCLIService", thriftCliService)
      addService(thriftCliService)
      initCompositeService(hiveConf)
    }

    private def isHTTPTransportMode(hiveConf: HiveConf): Boolean = {
      val transportMode = hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
      transportMode.toLowerCase(Locale.ENGLISH).equals("http")
    }


    override def start(): Unit = {
      super.start()
      started.set(true)
    }

    override def stop(): Unit = {
      if (started.getAndSet(false)) {
        super.stop()
      }
    }
  }
}

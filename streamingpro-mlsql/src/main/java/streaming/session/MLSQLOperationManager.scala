package streaming.session

import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.MLSQLConf._
import streaming.log.Logging

import scala.collection.JavaConverters._


/**
  * Created by allwefantasy on 4/6/2018.
  */
class MLSQLOperationManager(_interval: Int) extends Logging {
  private[this] val operations = new java.util.concurrent.ConcurrentHashMap[String, MLSQLOperation]()
  private val opCleaner =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())

  private[this] var execPool: ThreadPoolExecutor = _


  private[this] def createExecPool(): Unit = {
    val threadPoolName = "MLSQL-Background-Pool"
    execPool =
      new ThreadPoolExecutor(
        100,
        100,
        10,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable](100),
        new ThreadFactory {
          override def newThread(r: Runnable): Thread = {
            val t = new Thread(r)
            t.setName(threadPoolName + ": Thread-" + t.getId)
            t
          }
        })

    execPool.allowCoreThreadTimeOut(true)
  }

  def asyncRun(r: Runnable): Future[_] = execPool.submit(r)

  def createOp(session: MLSQLSession, f: () => Unit, opId: String, desc: String, operationTimeout: Int) = {

    val operation = new MLSQLOperation(session, f, opId, desc, operationTimeout)
    operations.put(operation.getOpId, operation)
    operation
  }


  def cancelOp(opName: String) = {
    val op = operations.get(opName)
    if (op != null) {
      op.cancel()
    }

  }

  def closeOp(opName: String) = {
    val op = operations.get(opName)
    if (op != null) {
      op.close()
    }

  }

  def start(): Unit = {
    // at least 1 minutes
    val interval = math.max(_interval, 60)
    log.info(s"Scheduling MLSQLOperationManager cache cleaning every $interval seconds")
    opCleaner.scheduleAtFixedRate(cleaner, interval, interval, TimeUnit.SECONDS)
    createExecPool()
  }

  def stop(): Unit = {
    log.info("Stopping SparkSession Cache Manager")
    opCleaner.shutdown()
    if (execPool != null) {
      execPool.shutdown()
      try {
        execPool.awaitTermination(30, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
          log.warn("ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + 30 +
            " seconds has been exceeded. RUNNING background operations will be shut down", e)
      }
      execPool = null
    }
    operations.asScala.foreach {
      case (_, op: MLSQLOperation) => op.close()
      case _ =>
    }
  }

  private[this] val cleaner = new Runnable {
    override def run(): Unit = {
      operations.asScala.foreach {
        case (_, op: MLSQLOperation) =>
          if (op.isTimedOut) {
            log.info(s"MLSQLSession job [${op.getOpId}] [${op.getDesc}]  is timeout, exceed ${op.getOperationTimeout}.")
            op.close()
          }
        case _ =>
      }
    }
  }
}

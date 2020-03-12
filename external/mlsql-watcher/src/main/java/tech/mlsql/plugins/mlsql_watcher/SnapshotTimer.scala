package tech.mlsql.plugins.mlsql_watcher

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.DataCompute
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.plugins.mlsql_watcher.db.PluginDB.ctx
import tech.mlsql.plugins.mlsql_watcher.db.PluginDB.ctx._
import tech.mlsql.plugins.mlsql_watcher.db._

/**
 * 10/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object SnapshotTimer extends Logging {


  private val timer =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(getClass.getSimpleName + "-%d").build())


  // default seven days
  val cleanerThresh = new AtomicLong(0)


  private[this] val snapshotSaver = new Runnable {
    override def run(): Unit = {
      if (!MLSQLWatcherCommand.isDbConfigured) {
        return
      }
      if (cleanerThresh.get() == 0) {
        val tempValue = ctx.run(query[WKv].filter(_.name == lift(MLSQLWatcherCommand.KEY_CLEAN_TIME))).headOption match {
          case Some(kv) => kv.value.toLong
          case None => 0
        }
        cleanerThresh.set(tempValue)
      }

      val (executorItems, executorJobItems) = DataCompute.compute()
      try {
        if (cleanerThresh.get() > 0) {
          ctx.run(query[WExecutor].filter(_.createdAt < lift(System.currentTimeMillis() - cleanerThresh.get())).delete)
          ctx.run(query[WExecutorJob].filter(_.createdAt < lift(System.currentTimeMillis() - cleanerThresh.get())).delete)
        }
        ctx.run(liftQuery(executorItems).foreach(item => query[WExecutor].insert(item)))
        ctx.run(liftQuery(executorJobItems).foreach(item => query[WExecutorJob].insert(item)))
      } catch {
        case e: Exception =>
          logError("Fail to save data", e)
      }

    }
  }

  final val isRun = new AtomicBoolean(false)


  def start(): Unit = {
    synchronized {
      if (!isRun.get()) {
        logInfo(s"Scheduler MLSQL state every 3 seconds")
        timer.scheduleAtFixedRate(snapshotSaver, 10, 5, TimeUnit.SECONDS)
        isRun.set(true)
      }
    }
  }

  def stop(): Unit = {
    logInfo("Shut down MLSQL state scheduler")
    timer.shutdown()
  }
}

package tech.mlsql.ets

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.delta.commands.CompactTableInDelta
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

import scala.collection.JavaConverters._

/**
  * 2019-06-06 WilliamZhu(allwefantasy@gmail.com)
  */
class DeltaCompactionCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    params.get(compactVersion.name).map { p =>
      set(compactVersion, p.toInt)
    }.getOrElse {
      throw new MLSQLException(s"${compactVersion.name} is required")
    }

    params.get(compactNumFilePerDir.name).map { p =>
      set(compactNumFilePerDir, p.toInt)
    }.getOrElse {
      throw new MLSQLException(s"${compactNumFilePerDir.name} is required")
    }

    params.get(compactRetryTimesForLock.name).map { p =>
      set(compactRetryTimesForLock, p.toInt)
    }.getOrElse {
      throw new MLSQLException(s"${compactRetryTimesForLock.name} is required")
    }

    val runInBackGround = params.getOrElse(background.name, "false").toBoolean
    val deltaLog = DeltaLog.forTable(spark, path)

    val optimizeTableInDelta = CompactTableInDelta(deltaLog,
      new DeltaOptions(Map[String, String](), df.sparkSession.sessionState.conf), Seq(), params)


    val runCancel = params.getOrElse(cancel.name, "false").toBoolean
    val threadName = s"Compaction-delta-${path}"
    Thread.getAllStackTraces.keySet.asScala.filter(_.getName == threadName).headOption match {
      case Some(compactionThread) =>
        if (runCancel) {
          compactionThread.interrupt()
        } else {
          throw new MLSQLException(s"There is a compaction thread for ${path} already. Please try again")
        }

      case None =>
    }


    if (runInBackGround) {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          optimizeTableInDelta.run(df.sparkSession)
        }
      })
      thread.setDaemon(true)
      thread.setName(threadName)
      thread.start()
    } else {
      optimizeTableInDelta.run(df.sparkSession)
    }

    val msg = if (runInBackGround) s"Compact ${path} in background" else s"Compact ${path} successfully"

    spark.createDataset[String](Seq(msg)).toDF("value")
  }


  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  final val compactVersion: Param[Int] = new Param[Int](this, "compactVersion", "")
  final val compactNumFilePerDir: Param[Int] = new Param[Int](this, "compactNumFilePerDir", "default 1")
  final val compactRetryTimesForLock: Param[Int] = new Param[Int](this, "compactRetryTimesForLock", "default 0")
  final val background: Param[Boolean] = new Param[Boolean](this, "background", "default true")
  final val cancel: Param[Boolean] = new Param[Boolean](this, "cancel", "default false")


}


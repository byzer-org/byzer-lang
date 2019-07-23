package tech.mlsql.ets

import org.apache.spark.SparkCoreVersion
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.CompactTableInDelta
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.mmlib.{Core_2_3_x, SQLAlg}

/**
  * 2019-06-06 WilliamZhu(allwefantasy@gmail.com)
  */
class DeltaCompactionCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    require(SparkCoreVersion.version > Core_2_3_x.coreVersion,
      s"Spark ${SparkCoreVersion.exactVersion} not support delta"
    )

    val spark = df.sparkSession

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

    val deltaLog = DeltaLog.forTable(spark, path)

    val optimizeTableInDelta = CompactTableInDelta(deltaLog,
      new DeltaOptions(Map[String, String](), df.sparkSession.sessionState.conf), Seq(), params)

    val items = optimizeTableInDelta.run(df.sparkSession)

    val acitons = items.map(f => Action.fromJson(f.getString(0)))
    val newFilesSize = acitons.filter(f => f.isInstanceOf[AddFile]).size
    val removeFilesSize = acitons.filter(f => f.isInstanceOf[RemoveFile]).size

    import spark.implicits._
    spark.createDataset[(String, Int)](Seq(("addNewFiles", newFilesSize), ("removeFiles", removeFilesSize))).toDF("name", "value")
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

}


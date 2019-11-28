package tech.mlsql.ets

import org.apache.spark.SparkCoreVersion
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.mmlib.{Core_2_3_x, SQLAlg}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.datalake.DataLake
import tech.mlsql.ets.delta.{DeltaUtils, TableStat}

/**
  * 2019-06-06 WilliamZhu(allwefantasy@gmail.com)
  */
class DeltaCommandWrapper(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    require(SparkCoreVersion.version > Core_2_3_x.coreVersion,
      s"Spark ${SparkCoreVersion.exactVersion} not support delta"
    )

    val spark = df.sparkSession
    import spark.implicits._

    def resolveRealPath(dataPath: String) = {
      val dataLake = new DataLake(spark)
      if (dataLake.isEnable) {
        dataLake.identifyToPath(dataPath)
      } else {
        PathFun(path).add(dataPath).toPath
      }
    }

    // !delta compact /data/table1 20 1 in background
    val command = JSONTool.parseJson[List[String]](params("parameters"))
    command match {
      case Seq("compact", dataPath, version, numFile, _*) =>

        val code =
          s"""
             |run command as DeltaCompactionCommand.`${resolveRealPath(dataPath)}`
             |where compactVersion="${version}"
             |and compactRetryTimesForLock="10"
             |and compactNumFilePerDir="${numFile}"
             |and background="false"
             |;
      """.stripMargin

        val runInBackGround = command.last == "background"

        var df: DataFrame = null
        if (runInBackGround) {
          ScriptRunner.runSubJobAsync(
            code, (df) => {}, Option(spark), false, false)
        } else {
          df = ScriptRunner.rubSubJob(
            code, (df) => {}, Option(spark), true, false).get
        }

        if (runInBackGround) spark.createDataset[String](Seq(s"Compact ${path} in background")).toDF("value") else {
          df
        }


      case Seq("history", dataPath, _*) =>

        val deltaLog = DeltaLog.forTable(spark, resolveRealPath(dataPath))
        val history = deltaLog.history.getHistory(Option(1000))
        spark.createDataset[CommitInfo](history).toDF()


      case Seq("info", dataPath, _*) =>
        val deltaLog = DeltaLog.forTable(spark, resolveRealPath(dataPath))
        val info = DeltaUtils.tableStat(deltaLog)
        spark.createDataset[TableStat](Seq(info)).toDF()


      case Seq("show", "tables") =>
        val dataLake = new DataLake(spark)
        if (!dataLake.isEnable) {
          throw new MLSQLException("datalake mode is not enabled.")
        } else {
          spark.createDataset(dataLake.listTables).toDF()
        }

      case Seq("help", _ *) =>
        spark.createDataset[String](Seq(
          """
            |!delta compact [tablePath] [compactVersion] [compactNumFilePerDir] [in background];
            |
            |`in background` is optional, and the other parameters is required.
            |
            |!delta history [tablePath];
            |!delta show tables;
          """.stripMargin)).toDF("value")
      case _ => throw new MLSQLException(
        """
          |please use `!delta help;` to get the usage.
        """.stripMargin)
    }


  }


  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }


}


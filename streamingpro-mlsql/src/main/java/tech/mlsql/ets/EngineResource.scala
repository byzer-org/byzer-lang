package tech.mlsql.ets

import org.apache.spark.SparkConf
import org.apache.spark.ml.param.Param
import org.apache.spark.scheduler.cluster._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod._


/**
  * 2019-04-26 WilliamZhu(allwefantasy@gmail.com)
  */
class EngineResource(override val uid: String) extends SQLAlg with ETAuth with Functions with WowParams with Logging with WowLog {
  def this() = this(BaseParams.randomUID())

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession

    val executorInfo = new SparkInnerExecutors(spark)
    val resourceControl = new SparkDynamicControlExecutors(spark)

    def isLocalMaster(conf: SparkConf): Boolean = {
      //      val master = MLSQLConf.MLSQL_MASTER.readFrom(configReader).getOrElse("")
      val master = conf.get("spark.master", "")
      master == "local" || master.startsWith("local[")
    }

    if (isLocalMaster(spark.sparkContext.getConf)) {
      throw new MLSQLException("Local mode not support this action");
    }

    if (!params.contains(action.name) || params(action.name).isEmpty) {
      import spark.implicits._
      return spark.createDataset[ResourceStatus](Seq(executorInfo.status)).toDF()
    }

    val _action = fetchParam(params, action, ParamConvertOption.nothing, ParamDefaultOption.required[String])
    val _cpus = parseCores(fetchParam[String](params, cpus, ParamConvertOption.nothing, ParamDefaultOption.required[String]))
    val _timeout = fetchParam[Int](params, timeout, ParamConvertOption.toInt, (_) => {
      set(timeout, 60 * 1000)
    })

    val executorsShouldAddOrRemove = Math.floor(_cpus / executorInfo.executorCores).toInt
    val currentExecutorNum = executorInfo.executorDataMap.size

    def tooMuchWithOneTime(cpusOrExecutorNum: Int) = {
      if (cpusOrExecutorNum > 20) {
        throw new MLSQLException("Too many cpus added at one time. Please add them with multi times.");
      }
    }

    parseAction(_action) match {
      case Action.+ | Action.ADD =>
        logInfo(s"Adding cpus; currentExecutorNum:${currentExecutorNum} targetExecutorNum:${currentExecutorNum + executorsShouldAddOrRemove}")
        tooMuchWithOneTime(_cpus)
        resourceControl.requestTotalExecutors(currentExecutorNum + executorsShouldAddOrRemove, _timeout)

      case Action.- | Action.REMOVE =>
        logInfo(s"Removing cpus; currentExecutorNum:${currentExecutorNum} targetExecutorNum:${currentExecutorNum - executorsShouldAddOrRemove}")
        tooMuchWithOneTime(_cpus)
        resourceControl.killExecutors(executorsShouldAddOrRemove, _timeout)
      case Action.SET =>

        val diff = executorsShouldAddOrRemove - currentExecutorNum
        if (diff < 0) {
          tooMuchWithOneTime(-diff)
          logInfo(s"Adding cpus; currentExecutorNum:${currentExecutorNum} targetExecutorNum:${currentExecutorNum + diff}")
          resourceControl.killExecutors(-diff, _timeout)
        }

        if (diff > 0) {
          tooMuchWithOneTime(diff)
          logInfo(s"Removing cpus; currentExecutorNum:${currentExecutorNum} targetExecutorNum:${executorsShouldAddOrRemove}")
          resourceControl.requestTotalExecutors(executorsShouldAddOrRemove, _timeout)
        }
    }

    import spark.implicits._
    spark.createDataset[ResourceStatus](Seq(executorInfo.status)).toDF()

  }

  def parseCores(str: String) = {
    if (str.toLowerCase.endsWith("c")) {
      str.toLowerCase.stripSuffix("c").toInt
    } else {
      str.toInt
    }
  }

  def parseAction(str: String) = {
    Action.withName(str)
  }

  override def skipPathPrefix: Boolean = true

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  object Action extends Enumeration {
    type action = Value
    val ADD = Value("add")
    val REMOVE = Value("remove")
    val + = Value("+")
    val - = Value("-")
    val SET = Value("set")
  }


  final val action: Param[String] = new Param[String](this, "action", "")
  final val cpus: Param[String] = new Param[String](this, "cpus", "")
  final val timeout: Param[Int] = new Param[Int](this, "timeout", "")

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {

    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__resource_allocate__"),
      OperateType.INSERT,
      Option("_mlsql_"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None => List(TableAuthResult(true, ""))
    }

  }

}


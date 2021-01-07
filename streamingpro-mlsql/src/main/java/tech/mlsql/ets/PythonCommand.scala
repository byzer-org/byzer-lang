package tech.mlsql.ets

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SparkUtils}
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}
import org.apache.spark.{MLSQLSparkUtils, SparkEnv, TaskContext, WowRowEncoder}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.arrow.python.PythonWorkerFactory
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction}
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, SocketServerInExecutor, SocketServerSerDer, TempSocketServerInDriver}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.net.NetTool
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.python._
import tech.mlsql.schema.parser.SparkSimpleSchemaParser
import tech.mlsql.session.SetSession
import tech.mlsql.tool.ScriptEnvDecode._

import scala.collection.mutable.ArrayBuffer

/**
 * 2019-08-16 WilliamZhu(allwefantasy@gmail.com)
 */
class PythonCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  final val inputTable: Param[String] = new Param[String](this, "inputTable", " ")
  final val code: Param[String] = new Param[String](this, "code", " ")
  final val outputTable: Param[String] = new Param[String](this, "outputTable", " ")

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val spark = df.sparkSession
    val context = ScriptSQLExec.context()

    val envSession = new SetSession(spark, context.owner)
    envSession.set("pythonMode", "python", Map(SetSession.__MLSQL_CL__ -> SetSession.PYTHON_RUNNER_CONF_CL))

    val hostAndPortContext = new AtomicReference[ReportHostAndPort]()
    val tempServer = new TempSocketServerInDriver(hostAndPortContext) {
      override def host: String = {
        if (SparkEnv.get == null || MLSQLSparkUtils.blockManager == null || MLSQLSparkUtils.blockManager.blockManagerId == null) {
          NetTool.localHostName()
        }
        else MLSQLSparkUtils.blockManager.blockManagerId.host
      }
    }

    val tempSocketServerHost = tempServer._host
    val tempSocketServerPort = tempServer._port

    val timezoneID = spark.sessionState.conf.sessionLocalTimeZone

    def launchPythonServer = {
      val serverId = "python-runner-${UUID.randomUUID().toString}"
      spark.sparkContext.setJobGroup(serverId, s"python runner ${serverId} owned by ${context.owner}", true)
      spark.sparkContext.parallelize(Seq(serverId), 1).map { item =>

        val taskContextRef: AtomicReference[TaskContext] = new AtomicReference[TaskContext]()
        taskContextRef.set(TaskContext.get())

        val executorPythonServer = new PythonServer(taskContextRef)


        def sendStopServerRequest = {
          // send signal to stop server
          val client = new SocketServerSerDer[PythonSocketRequest, PythonSocketResponse]() {}
          val socket2 = new Socket(executorPythonServer._host, executorPythonServer._port)
          val dout2 = new DataOutputStream(socket2.getOutputStream)
          client.sendRequest(dout2,
            ShutDownPythonServer())
          socket2.close()
        }

        TaskContext.get().addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            taskContextRef.set(null)
            sendStopServerRequest

          }
        })

        TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            taskContextRef.set(null)
            sendStopServerRequest
          }
        })

        SocketServerInExecutor.reportHostAndPort(tempSocketServerHost,
          tempSocketServerPort,
          ReportHostAndPort(executorPythonServer._host, executorPythonServer._port))

        while (!TaskContext.get().isInterrupted() && !executorPythonServer.isClosed) {
          Thread.sleep(1000)
        }

        ""
      }.collect()
      try {
        tempServer._server.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }

    }


    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray

    def execute(code: String, table: Option[String]) = {
      val connect = PythonServerHolder.fetch(context.owner).get
      val runnerConf = getSchemaAndConf(envSession) ++ configureLogConf

      // 1. make sure we are in interactive mode
      // 2. make sure the user is configured so every one has his own python worker
      val envs = Map(
        ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER) -> context.owner,
        ScalaMethodMacros.str(PythonConf.PY_INTERACTIVE) -> "yes",
        ScalaMethodMacros.str(PythonConf.PYTHON_ENV) -> "export ARROW_PRE_0_15_IPC_FORMAT=1"
      ) ++
        envSession.fetchPythonEnv.get.collect().map { f =>
          if (f.k == ScalaMethodMacros.str(PythonConf.PYTHON_ENV)) {
            (f.k, f.v + " && export ARROW_PRE_0_15_IPC_FORMAT=1")
          } else {
            (f.k, f.v)
          }

        }.toMap


      def request = {
        // send signal to stop server
        val client = new SocketServerSerDer[PythonSocketRequest, PythonSocketResponse]() {}
        val socket2 = new Socket(connect.host, connect.port)
        val dout2 = new DataOutputStream(socket2.getOutputStream)
        val din2 = new DataInputStream(socket2.getInputStream)

        client.sendRequest(dout2,
          ExecuteCode(code, envs, runnerConf, timezoneID))
        val res = client.readResponse(din2).asInstanceOf[ExecuteResult]
        socket2.close()
        res
      }

      val res = request
      val df = if (res.ok) {
        val schema = SparkSimpleSchemaParser.parse(runnerConf("schema")).asInstanceOf[StructType]
        spark.createDataFrame(spark.sparkContext.parallelize(request.a.map { item => Row.fromSeq(item) }), schema)
      } else {
        val e = new MLSQLException(request.a.map { item => item.headOption.getOrElse("") }.mkString("\n"))
        recognizeError(e)
      }
      table.map(df.createOrReplaceTempView(_))
      df
    }

    val newdf = command match {
      case Array("start") =>
        if (!PythonServerHolder.fetch(context.owner).isDefined) {
          // start the server
          new Thread("launch-python-socket-server-in-spark-job") {
            setDaemon(true)

            override def run(): Unit = {
              launchPythonServer
            }
          }.start()

          var count = 60

          while (hostAndPortContext.get() == null) {
            Thread.sleep(1000)
            count -= 1
          }
          if (hostAndPortContext.get() == null) {
            throw new RuntimeException("start Python worker fail")
          }
          PythonServerHolder.add(context.owner, hostAndPortContext.get())
        }
        emptyDataFrame(spark, "value")


      case Array("close") =>

        PythonServerHolder.close(context.owner)

        emptyDataFrame(spark, "value")

      case Array("env", kv) => // !python env a=b
        val Array(k, v) = decode(kv).split("=", 2)
        envSession.set(k, v, Map(SetSession.__MLSQL_CL__ -> SetSession.PYTHON_ENV_CL))
        envSession.fetchPythonEnv.get.toDF()

      case Array("conf", kv) => // !python env a=b
        val Array(k, v) = decode(kv).split("=", 2)
        envSession.set(k, v, Map(SetSession.__MLSQL_CL__ -> SetSession.PYTHON_RUNNER_CONF_CL))
        envSession.fetchPythonRunnerConf.get.toDF()

      case Array(code, "named", tableName) =>
        execute(code, Option(tableName))

      case Array("on", tableName, code) =>
        distribute_execute(spark, code, tableName)

      case Array("on", tableName, code, "named", targetTable) =>
        val resDf = distribute_execute(spark, code, tableName)
        resDf.createOrReplaceTempView(targetTable)
        resDf

      case Array(code) => execute(code, None)


    }
    newdf
  }

  private def getSchemaAndConf(envSession: SetSession) = {
    def error = {
      throw new MLSQLException(
        """
          |Using `!python conf` to specify the python return value format is required.
          |Do like following:
          |
          |```
          |!python conf "schema=st(field(a,integer),field(b,integer))"
          |```
        """.stripMargin)
    }

    val runnerConf = envSession.fetchPythonRunnerConf match {
      case Some(conf) =>
        val temp = conf.collect().map(f => (f.k, f.v)).toMap
        if (!temp.contains("schema")) {
          error
        }
        temp
      case None => error
    }
    runnerConf
  }

  private def distribute_execute(session: SparkSession, code: String, sourceTable: String) = {
    import scala.collection.JavaConverters._
    val context = ScriptSQLExec.context()
    val envSession = new SetSession(session, context.owner)
    val envs = Map(
      ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER) -> context.owner,
      ScalaMethodMacros.str(PythonConf.PYTHON_ENV) -> "export ARROW_PRE_0_15_IPC_FORMAT=1"
    ) ++
      envSession.fetchPythonEnv.get.collect().map { f =>
        if (f.k == ScalaMethodMacros.str(PythonConf.PYTHON_ENV)) {
          (f.k, f.v + " && export ARROW_PRE_0_15_IPC_FORMAT=1")
        } else {
          (f.k, f.v)
        }

      }.toMap

    val runnerConf = getSchemaAndConf(envSession) ++ configureLogConf

    val targetSchema = runnerConf("schema").trim match {
      case item if item.startsWith("{") =>
        DataType.fromJson(runnerConf("schema")).asInstanceOf[StructType]
      case item if item.startsWith("st") =>
        SparkSimpleSchemaParser.parse(runnerConf("schema")).asInstanceOf[StructType]
      case _ =>
        StructType.fromDDL(runnerConf("schema"))
    }

    val pythonVersion = runnerConf.getOrElse("pythonVersion", "3.6")
    val timezoneID = session.sessionState.conf.sessionLocalTimeZone
    val df = session.table(sourceTable)
    val sourceSchema = df.schema
    try {
      val data = df.rdd.mapPartitions { iter =>

        val encoder = WowRowEncoder.fromRow(sourceSchema)
        val envs4j = new util.HashMap[String, String]()
        envs.foreach(f => envs4j.put(f._1, f._2))

        val batch = new ArrowPythonRunner(
          Seq(ChainedPythonFunctions(Seq(PythonFunction(
            code, envs4j, "python", pythonVersion)))), sourceSchema,
          timezoneID, runnerConf
        )

        val newIter = iter.map { irow =>
          encoder(irow).copy()
        }
        val commonTaskContext = new SparkContextImp(TaskContext.get(), batch)
        val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
        columnarBatchIter.flatMap { batch =>
          batch.rowIterator.asScala
        }
      }

      SparkUtils.internalCreateDataFrame(session, data, targetSchema, false)
    } catch {
      case e: Exception =>
        recognizeError(e)
    }


  }

  def isLocalMaster(conf: Map[String, String]): Boolean = {
    //      val master = MLSQLConf.MLSQL_MASTER.readFrom(configReader).getOrElse("")
    val master = conf.getOrElse("spark.master", "")
    master == "local" || master.startsWith("local[")
  }

  /**
   *
   * Here we should give mlsql log server information to the conf which
   * will be configured by ArrowPythonRunner
   */
  private def configureLogConf() = {
    val context = ScriptSQLExec.context()
    val conf = context.execListener.sparkSession.sqlContext.getAllConfs
    val extraConfig = if (!conf.contains("spark.mlsql.log.driver.host")) {
      Map[String, String]()
    } else {
      Map(PythonWorkerFactory.Tool.REDIRECT_IMPL -> "tech.mlsql.log.RedirectStreamsToSocketServer")
    }
    conf.filter(f => f._1.startsWith("spark.mlsql.log.driver")) ++
      Map(
        ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER) -> context.owner,
        "groupId" -> context.groupId
      ) ++ extraConfig
  }

  private def recognizeError(e: Exception) = {
    val buffer = ArrayBuffer[String]()
    format_full_exception(buffer, e, true)
    val typeError = buffer.filter(f => f.contains("Previous exception in task: null")).filter(_.contains("org.apache.spark.sql.vectorized.ArrowColumnVector$ArrowVectorAccessor")).size > 0
    if (typeError) {
      throw new MLSQLException(
        """
          |We can not reconstruct data from Python.
          |Try to use !python conf "schema=" change your schema.
        """.stripMargin, e)
    }
    throw e
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def skipPathPrefix: Boolean = true

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new MLSQLException(s"${getClass.getName} not support register ")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new MLSQLException(s"${getClass.getName} not support register ")
}

object PythonServerHolder {
  private val obj = new java.util.concurrent.ConcurrentHashMap[String, java.util.ArrayList[ReportHostAndPort]]()

  def add(ower: String, info: ReportHostAndPort) = {
    val temp = obj.get(ower)
    if (temp == null) {
      val items = new util.ArrayList[ReportHostAndPort]()
      items.add(info)
      obj.put(ower, items)
    } else {
      temp.add(info)
    }
  }

  def fetch(owner: String) = {
    val temp = obj.get(owner)
    if (temp == null) None
    else Option(temp.get(0))
  }

  def close(owner: String) = {
    val temp = obj.remove(owner)

    def sendStopServerRequest = {
      val executorPythonServer = temp.get(0)
      // send signal to stop server
      val client = new SocketServerSerDer[PythonSocketRequest, PythonSocketResponse]() {}
      val socket2 = new Socket(executorPythonServer.host, executorPythonServer.port)
      val dout2 = new DataOutputStream(socket2.getOutputStream)
      client.sendRequest(dout2,
        ShutDownPythonServer())
      socket2.close()
    }

    sendStopServerRequest

  }
}

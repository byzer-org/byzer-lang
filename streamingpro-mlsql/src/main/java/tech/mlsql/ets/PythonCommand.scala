package tech.mlsql.ets

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}
import org.apache.spark.{MLSQLSparkUtils, TaskContext}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, SocketServerInExecutor, SocketServerSerDer, TempSocketServerInDriver}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.python._
import tech.mlsql.schema.parser.SparkSimpleSchemaParser
import tech.mlsql.session.SetSession

/**
  * 2019-08-16 WilliamZhu(allwefantasy@gmail.com)
  */
class PythonCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val spark = df.sparkSession
    val context = ScriptSQLExec.context()

    val hostAndPortContext = new AtomicReference[ReportHostAndPort]()
    val tempServer = new TempSocketServerInDriver(hostAndPortContext) {
      override def host: String = MLSQLSparkUtils.rpcEnv().address.host
    }

    val tempSocketServerHost = tempServer._host
    val tempSocketServerPort = tempServer._port


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

    val envSession = new SetSession(spark, context.owner)
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray

    def execute(code: String, table: Option[String]) = {
      val connect = PythonServerHolder.fetch(context.owner).get
      envSession.fetchPythonEnv match {
        case None => throw new MLSQLException(
          """
            |Env schema and PYTHON_ENV should be set, e.g.
            |
            |1. schema=st(field(a,integer),field(b,integer));
            |2. PYTHON_ENV=source activate streamingpro-spark-2.4.x
            |
            |Or if you do not use conda, please you can set like this:
            |
            |PYTHON_ENV=echo yes
          """.stripMargin)
        case Some(i) =>
      }
      val envs = envSession.fetchPythonEnv.get.collect().map(f => (f.k, f.v)).toMap

      def request = {
        // send signal to stop server
        val client = new SocketServerSerDer[PythonSocketRequest, PythonSocketResponse]() {}
        val socket2 = new Socket(connect.host, connect.port)
        val dout2 = new DataOutputStream(socket2.getOutputStream)
        val din2 = new DataInputStream(socket2.getInputStream)

        client.sendRequest(dout2,
          ExecuteCode(code, envs - "schema", envs("schema")))
        val res = client.readResponse(din2).asInstanceOf[ExecuteResult]
        socket2.close()
        res
      }

      val res = request
      val df = if (res.ok) {
        val schema = SparkSimpleSchemaParser.parse(envs("schema")).asInstanceOf[StructType]
        spark.createDataFrame(spark.sparkContext.parallelize(request.a.map { item => Row.fromSeq(item) }), schema)
      } else {
        throw new MLSQLException(request.a.map { item => item.headOption.getOrElse("") }.mkString("\n"))
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
        val Array(k, v) = kv.split("=", 2)
        envSession.set(k, v, Map(SetSession.__MLSQL_CL__ -> SetSession.PYTHON_ENV_CL))
        envSession.fetchPythonEnv.get.toDF()

      case Array(code, "named", tableName) =>
        execute(code, Option(tableName))
      case Array(code) => execute(code, None)


    }
    newdf
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

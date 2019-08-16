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
import tech.mlsql.common.utils.Md5
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, SocketServerInExecutor, SocketServerSerDer, TempSocketServerInDriver}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.python._
import tech.mlsql.schema.parser.SparkSimpleSchemaParser

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
    }

    def envTableName = {
      Md5.md5Hash(context.owner)
    }

    import spark.implicits._
    val command = JSONTool.parseJson[List[String]](params("parameters")).toArray
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

        spark.catalog.dropTempView(envTableName)
        PythonServerHolder.close(context.owner)

        emptyDataFrame(spark, "value")

      case Array("env", kv) => // !python env a=b
        addItemToTable(spark, envTableName, kv)
        spark.table(envTableName)

      case Array(code) =>
        val connect = PythonServerHolder.fetch(context.owner).get

        def getEnvs = {
          if (spark.catalog.tableExists(envTableName)) {
            spark.table(envTableName).as[String].collect().toSeq.map(f => f.split("=")).map(a => (a(0), a(1))).toMap
          } else Map[String, String]()

        }

        val envs = getEnvs

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

        val schema = SparkSimpleSchemaParser.parse(envs("schema")).asInstanceOf[StructType]
        spark.createDataFrame(spark.sparkContext.parallelize(request.a.map { item => Row.fromSeq(item) }), schema)
    }
    newdf
  }


  def addItemToTable(spark: SparkSession, table: String, item: String) = {
    import spark.implicits._
    if (spark.catalog.tableExists(table)) {
      spark.createDataset[String](spark.table(table).as[String].collect().toSeq ++ Seq(item)).toDF("value").
        createOrReplaceTempView(table)
    } else {
      spark.createDataset[String](Seq(item)).toDF("value").
        createOrReplaceTempView(table)
    }
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

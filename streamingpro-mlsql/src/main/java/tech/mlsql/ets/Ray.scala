package tech.mlsql.ets

import java.net.{InetAddress, ServerSocket}
import java.util
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SparkUtils}
import org.apache.spark.{MLSQLSparkUtils, SparkEnv, SparkInstanceService, TaskContext, WowRowEncoder}
import streaming.core.datasource.util.MLSQLJobCollect
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.arrow.python.PythonWorkerFactory
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner._
import tech.mlsql.common.utils.base.TryTool
import tech.mlsql.common.utils.distribute.socket.server.{ReportHostAndPort, SocketServerInExecutor}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.net.NetTool
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.ray.{CollectServerInDriver, DataServer}
import tech.mlsql.log.WriteLog
import tech.mlsql.schema.parser.SparkSimpleSchemaParser
import tech.mlsql.session.SetSession
import tech.mlsql.version.VersionCompatibility

import scala.collection.mutable.ArrayBuffer

/**
 * 24/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class Ray(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  //
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession

    def genCode(code: String) = {
      if (code.startsWith("py.")) {
        val content = spark.table(code.split("\\.").last).collect().head.getString(0)
        val value = JSONTool.parseJson[Map[String, String]](content)
        value("content")
      } else code
    }

    val envSession = new SetSession(spark, ScriptSQLExec.context().owner)
    envSession.set("pythonMode", "ray", Map(SetSession.__MLSQL_CL__ -> SetSession.PYTHON_RUNNER_CONF_CL))


    val newdf = Array(params(inputTable.name), params(code.name), params(outputTable.name)) match {
      case Array(input, code, "") =>
        distribute_execute(spark, genCode(code), input)

      case Array(input, code, output) =>
        val resDf = distribute_execute(spark, genCode(code), input)
        resDf.createOrReplaceTempView(output)
        resDf
    }
    newdf
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
    val timezoneID = session.sessionState.conf.sessionLocalTimeZone
    val df = session.table(sourceTable)

    val refs = new AtomicReference[ArrayBuffer[ReportHostAndPort]]()
    refs.set(ArrayBuffer[ReportHostAndPort]())
    val stopFlag = new AtomicReference[String]()
    stopFlag.set("false")
    val tempSocketServerInDriver = new CollectServerInDriver(refs, stopFlag)


    var targetLen = df.rdd.partitions.length


    val tempdf = TryTool.tryOrElse {
      val resource = new SparkInstanceService(session).resources
      val jobInfo = new MLSQLJobCollect(session, context.owner)
      val leftResource = resource.totalCores - jobInfo.resourceSummary(null).activeTasks
      logInfo(s"RayMode: Resource:[${leftResource}(${resource.totalCores}-${jobInfo.resourceSummary(null).activeTasks})] TargetLen:[${targetLen}]")
      if (leftResource / 2 <= targetLen) {
        df.repartition(Math.max(Math.floor(leftResource / 2) - 1, 1).toInt)
      } else df
    } {
      //      WriteLog.write(List("Warning: Fail to detect instance resource. Setup 4 data server for Python.").toIterator, runnerConf)
      logWarning(format("Warning: Fail to detect instance resource. Setup 4 data server for Python."))
      if (targetLen > 4) {
        df.repartition(4)
      } else df
    }

    targetLen = tempdf.rdd.partitions.length
    logInfo(s"RayMode: Final TargetLen ${targetLen}")
    val _owner = ScriptSQLExec.context().owner

    val thread = new Thread("temp-data-server-in-spark") {
      override def run(): Unit = {

        val dataSchema = df.schema
        val tempSocketServerHost = tempSocketServerInDriver._host
        val tempSocketServerPort = tempSocketServerInDriver._port
        val timezoneID = session.sessionState.conf.sessionLocalTimeZone
        val owner = _owner
        tempdf.rdd.mapPartitions { iter =>

          val host: String = if (SparkEnv.get == null || MLSQLSparkUtils.blockManager == null || MLSQLSparkUtils.blockManager.blockManagerId == null) {
            WriteLog.write(List("Ray: Cannot get MLSQLSparkUtils.rpcEnv().address, using NetTool.localHostName()").iterator,
              Map("PY_EXECUTE_USER" -> owner))
            NetTool.localHostName()
          } else if (SparkEnv.get != null && SparkEnv.get.conf.getBoolean("spark.mlsql.deploy.on.k8s", false)) {
            InetAddress.getLocalHost.getHostAddress
          }
          else MLSQLSparkUtils.blockManager.blockManagerId.host

          val socketRunner = new SparkSocketRunner("serveToStreamWithArrow", host, timezoneID)
          val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
          val convert = WowRowEncoder.fromRow(dataSchema)
          val newIter = iter.map { irow =>
            convert(irow)
          }
          val Array(_server, _host, _port) = socketRunner.serveToStreamWithArrow(newIter, dataSchema, 1000, commonTaskContext)

          // send server info back
          SocketServerInExecutor.reportHostAndPort(tempSocketServerHost,
            tempSocketServerPort,
            ReportHostAndPort(_host.toString, _port.toString.toInt))

          while (_server != null && !_server.asInstanceOf[ServerSocket].isClosed) {
            Thread.sleep(1 * 1000)
          }
          List[String]().iterator
        }.count()
        logInfo("Exit all data server")
      }
    }
    thread.setDaemon(true)
    thread.start()

    var clockTimes = 120
    while (targetLen != refs.get().length && clockTimes >= 0) {
      Thread.sleep(500)
      clockTimes -= 1
    }
    if (clockTimes < 0) {
      throw new RuntimeException(s"fail to start data socket server. targetLen:${targetLen} actualLen:${refs.get().length}")
    }
    tempSocketServerInDriver.shutdown

    def isSchemaJson() = {
      runnerConf("schema").trim.startsWith("{")
    }

    def isSimpleSchema() = {
      runnerConf("schema").trim.startsWith("st")
    }

    val targetSchema = runnerConf("schema").trim match {
      case item if item.startsWith("{") =>
        DataType.fromJson(runnerConf("schema")).asInstanceOf[StructType]
      case item if item.startsWith("st") =>
        SparkSimpleSchemaParser.parse(runnerConf("schema")).asInstanceOf[StructType]
      case _ =>
        StructType.fromDDL(runnerConf("schema"))
    }


    val dataModeSchema = StructType(Seq(StructField("host", StringType), StructField("port", LongType), StructField("server_id", StringType)))
    val dataMode = runnerConf.getOrElse("dataMode", "model")

    val stage1_schema = if (dataMode == "data") dataModeSchema else targetSchema
    val stage2_schema = targetSchema


    import session.implicits._
    val runIn = runnerConf.getOrElse("runIn", "executor")
    val pythonVersion = runnerConf.getOrElse("pythonVersion", "3.6")
    val newdf = session.createDataset[DataServer](refs.get().map(f => DataServer(f.host, f.port, timezoneID))).repartition(1)
    val sourceSchema = newdf.schema
    val outputDF = runIn match {
      case "executor" =>
        val data = newdf.toDF().rdd.mapPartitions { iter =>
          val convert = WowRowEncoder.fromRow(sourceSchema)
          val envs4j = new util.HashMap[String, String]()
          envs.foreach(f => envs4j.put(f._1, f._2))

          val batch = new ArrowPythonRunner(
            Seq(ChainedPythonFunctions(Seq(PythonFunction(
              code, envs4j, "python", pythonVersion)))), sourceSchema,
            timezoneID, runnerConf
          )

          val newIter = iter.map { irow =>
            convert(irow).copy()
          }
          val commonTaskContext = new SparkContextImp(TaskContext.get(), batch)
          val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)
          columnarBatchIter.flatMap { batch =>
            batch.rowIterator.asScala
          }
        }

        SparkUtils.internalCreateDataFrame(session, data, stage1_schema, false)
      case "driver" =>
        val dataServers = refs.get().map(f => DataServer(f.host, f.port, timezoneID))
        val convert = WowRowEncoder.fromRow(sourceSchema)
        val stage1_schema_encoder = WowRowEncoder.toRow(stage1_schema)
        val envs4j = new util.HashMap[String, String]()
        envs.foreach(f => envs4j.put(f._1, f._2))

        val batch = new ArrowPythonRunner(
          Seq(ChainedPythonFunctions(Seq(PythonFunction(
            code, envs4j, "python", pythonVersion)))), sourceSchema,
          timezoneID, runnerConf
        )

        val newIter = dataServers.map { irow =>
          convert(Row.fromSeq(Seq(irow.host, irow.port, irow.timezone))).copy()
        }.iterator
        val javaContext = new JavaContext()
        val commonTaskContext = new AppContextImpl(javaContext, batch)
        val columnarBatchIter = batch.compute(Iterator(newIter), 0, commonTaskContext)
        val data = columnarBatchIter.flatMap { batch =>
          batch.rowIterator.asScala.map(f =>
            stage1_schema_encoder(f)
          )
        }.toList
        javaContext.markComplete
        javaContext.close
        val rdd = session.sparkContext.makeRDD[Row](data)
        session.createDataFrame(rdd, stage1_schema)
    }

    if (dataMode == "data") {
      val rows = outputDF.collect()
      val rdd = session.sparkContext.makeRDD[Row](rows, numSlices = rows.length)
      val stage2_schema_encoder = WowRowEncoder.fromRow(stage2_schema)
      val newRDD = if (rdd.partitions.length > 0 && rows.length > 0) {
        rdd.flatMap { row =>
          val socketRunner = new SparkSocketRunner("readFromStreamWithArrow", NetUtils.getHost, timezoneID)
          val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
          val pythonWorkerHost = row.getAs[String]("host")
          val pythonWorkerPort = row.getAs[Long]("port").toInt
          logInfo(s" Ray On Data Mode: connect python worker[${pythonWorkerHost}:${pythonWorkerPort}] ")
          val iter = socketRunner.readFromStreamWithArrow(pythonWorkerHost, pythonWorkerPort, commonTaskContext)
          iter.map(f => f.copy())
        }
      } else rdd.map(f => stage2_schema_encoder(f))

      SparkUtils.internalCreateDataFrame(session, newRDD, stage2_schema, false)
    } else {
      outputDF
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


  final val inputTable: Param[String] = new Param[String](this, "inputTable", " ")
  final val code: Param[String] = new Param[String](this, "code", " ")
  final val outputTable: Param[String] = new Param[String](this, "outputTable", " ")

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0")
  }


  override def doc: Doc = Doc(MarkDownDoc,
    s""" """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    s""" """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

}





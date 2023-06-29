package tech.mlsql.ets


import java.util
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SessionWrapper, SparkSession, SparkUtils}
import org.apache.spark.{TaskContext, WowRowEncoder}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.arrow.python.PythonWorkerFactory
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner._
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.ray.DataServer
import tech.mlsql.schema.parser.SparkSimpleSchemaParser
import tech.mlsql.session.SetSession
import tech.mlsql.tool.MasterSlaveInSpark
import tech.mlsql.version.VersionCompatibility

/**
 * 24/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class Ray(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  def genCode(spark: SparkSession, code: String) = {
    if (code.startsWith("py.")) {
      val content = spark.table(code.split("\\.").last).collect().head.getString(0)
      val value = JSONTool.parseJson[Map[String, String]](content)
      value("content")
    } else code
  }


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession


    val envSession = new SetSession(spark, ScriptSQLExec.context().owner)
    envSession.set("pythonMode", "ray", Map(SetSession.__MLSQL_CL__ -> SetSession.PYTHON_RUNNER_CONF_CL))

    val newdf = Array(params(inputTable.name), params(code.name), params(outputTable.name)) match {
      case Array(input, code, "") =>
        distribute_execute(spark, genCode(spark, code), input, params)

      case Array(input, code, output) =>
        val resDf = distribute_execute(spark, genCode(spark, code), input, params)
        resDf.createOrReplaceTempView(output)
        resDf
    }
    newdf
  }

  private def schemaFromStr(schemaStr: String) = {
    val targetSchema = schemaStr.trim match {
      case item if item.startsWith("{") =>
        DataType.fromJson(schemaStr).asInstanceOf[StructType]
      case item if item.startsWith("st") =>
        SparkSimpleSchemaParser.parse(schemaStr).asInstanceOf[StructType]
      case item if item == "file" =>
        SparkSimpleSchemaParser.parse("st(field(start,long),field(offset,long),field(value,binary))").asInstanceOf[StructType]
      case _ =>
        StructType.fromDDL(schemaStr)
    }
    targetSchema
  }


  private def distribute_execute(session: SparkSession, code: String, sourceTable: String, etParams: Map[String, String], extraConf: Map[String, String] = Map()) = {
    import scala.collection.JavaConverters._
    val context = ScriptSQLExec.context()
    val envSession = new SetSession(session, context.owner)
    val envs = Map(
      ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER) -> context.owner
    ) ++
      envSession.fetchPythonEnv.get.collect().map { f =>
        (f.k, f.v)
      }.toMap


    val confTableValue = etParams.get(confTable.name).map(t => session.table(t).collect().map { r =>
      (r.getString(0), r.getString(1))
    }.toMap).getOrElse(Map[String, String]())

    var runnerConf = Map(
      "HOME" -> context.home,
      "OWNER" -> context.owner,
      "GROUP_ID" -> context.groupId) ++ getSchemaAndConf(envSession) ++ configureLogConf ++ confTableValue ++ extraConf

    val timezoneID = session.sessionState.conf.sessionLocalTimeZone
    val df = session.table(sourceTable)

    val modelWaitServerReadyTimeout = etParams.getOrElse("modelWaitServerReadyTimeout", "300").toInt
    val dataWaitServerReadyTimeout = etParams.getOrElse("dataWaitServerReadyTimeout", "300").toInt

    // start spark data servers for model if user configure model table in et params.
    val modelTableOpt = etParams.get("model")
    if (modelTableOpt.isDefined) {
      val modelDf = session.table(modelTableOpt.get)
      val modelServer = new MasterSlaveInSpark("temp-model-server-in-spark", session, context.owner)
      modelServer.build(modelDf,etParams, MasterSlaveInSpark.defaultDataServerImpl)
      modelServer.waitWithTimeout(modelWaitServerReadyTimeout,"model server")
      runnerConf ++= Map("modelServers" -> modelServer.dataServers.get().map(item => s"${item.host}:${item.port}").mkString(","))
    }

    // start spark data servers for dataset
    val masterSlaveInSpark = new MasterSlaveInSpark("temp-data-server-in-spark", session, context.owner)
    masterSlaveInSpark.build(df,etParams, MasterSlaveInSpark.defaultDataServerImpl)
    masterSlaveInSpark.waitWithTimeout(dataWaitServerReadyTimeout,"data server")


    val targetSchema = schemaFromStr(runnerConf("schema"))


    val dataModeSchema = StructType(Seq(StructField("host", StringType), StructField("port", LongType), StructField("server_id", StringType)))
    val dataMode = runnerConf.getOrElse("dataMode", "model")

    val stage1_schema = if (dataMode == "data") dataModeSchema else targetSchema
    val stage2_schema = targetSchema
    val runIn = runnerConf.getOrElse("runIn", "executor")
    val pythonVersion = runnerConf.getOrElse("pythonVersion", "3.6")
    val pythonExec = runnerConf.getOrElse("pythonExec", "python")
    import session.implicits._
    val newdf = session.createDataset[DataServer](masterSlaveInSpark.dataServers.get().map(f => DataServer(f.host, f.port, timezoneID))).repartition(1)
    val sourceSchema = newdf.schema
    val outputDF = runIn match {
      case "executor" =>
        val data = newdf.toDF().rdd.mapPartitions { iter =>
          val convert = WowRowEncoder.fromRow(sourceSchema)
          val envs4j = new util.HashMap[String, String]()
          envs.foreach(f => envs4j.put(f._1, f._2))

          val batch = new ArrowPythonRunner(
            Seq(ChainedPythonFunctions(Seq(PythonFunction(
              code, envs4j, pythonExec, pythonVersion)))), sourceSchema,
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
        val dataServers = masterSlaveInSpark.dataServers.get().map(f => DataServer(f.host, f.port, timezoneID))
        val convert = WowRowEncoder.fromRow(sourceSchema)
        val stage1_schema_encoder = WowRowEncoder.toRow(stage1_schema)
        val envs4j = new util.HashMap[String, String]()
        envs.foreach(f => envs4j.put(f._1, f._2))

        val batch = new ArrowPythonRunner(
          Seq(ChainedPythonFunctions(Seq(PythonFunction(
            code, envs4j, pythonExec, pythonVersion)))), sourceSchema,
          timezoneID, runnerConf
        )

        val newIter = dataServers.map { irow =>
          convert(Row.fromSeq(Seq(irow.host, irow.port, irow.timezone))).copy()
        }.iterator
        val javaContext = new JavaContext()
        try {
          val commonTaskContext = new AppContextImpl(javaContext, batch)
          val columnarBatchIter = batch.compute(Iterator(newIter), 0, commonTaskContext)
          val data = columnarBatchIter.flatMap { batch =>
            batch.rowIterator.asScala.map(f =>
              stage1_schema_encoder(f)
            )
          }
          val wrapper = new SessionWrapper(session)
          wrapper.createDataFrame(data, stage1_schema)
        } catch {
          case e: Exception =>
            throw new MLSQLException("An exception was encountered in the execution of this python task! " +
              "Please check your code and try again after modification.", e)
        } finally {
          javaContext.markComplete
          javaContext.close
        }
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
      //      Map[String,String]()
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
  final val confTable: Param[String] = new Param[String](this, "confTable", " ")

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0")
  }


  override def doc: Doc = Doc(MarkDownDoc,
    s""" """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    s""" """.stripMargin)

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(session: SparkSession, path: String, params: Map[String, String]): Any = {
    return path
  }

  override def predict(session: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {

    def _load = {
      val registerCode = params.getOrElse("registerCode", "")
      val envSession = new SetSession(session, ScriptSQLExec.context().owner)
      envSession.set("pythonMode", "ray", Map(SetSession.__MLSQL_CL__ -> SetSession.PYTHON_RUNNER_CONF_CL))
      distribute_execute(
        session,
        genCode(session, registerCode),
        "command",
        params ++ Map("model" -> _model.toString), extraConf = Map("UDF_CLIENT" -> name)).collect()
    }

    // if the user try to reconnect to udf cluster in ray
    // there is no need to execute the registerCode since we
    // only need to register the udf in Byzer.
    if (!params.getOrElse("reconnect","false").toBoolean){
      _load
    }

    val predictCode = params.getOrElse("predictCode",
      """
        |import ray
        |from pyjava.api.mlsql import RayContext
        |from pyjava.udf import UDFMaster,UDFWorker,UDFBuilder,UDFBuildInFunc
        |
        |ray_context = RayContext.connect(globals(), context.conf["rayAddress"])
        |UDFBuilder.apply(ray_context)
        |""".stripMargin)
    val context = ScriptSQLExec.context()
    val envSession = new SetSession(session, context.owner)
    envSession.set("pythonMode", "ray", Map(SetSession.__MLSQL_CL__ -> SetSession.PYTHON_RUNNER_CONF_CL))
    val envs = Map(
      ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER) -> context.owner,
    ) ++
      envSession.fetchPythonEnv.get.collect().map { f =>
        (f.k, f.v)
      }.toMap

    val envs4j = new util.HashMap[String, String]()
    envs.foreach(f => envs4j.put(f._1, f._2))

    // this code make sure we get different python factory to create python worker.
    envs4j.put("UDF_CLIENT", name)

    val timezoneID = session.sessionState.conf.sessionLocalTimeZone

    val runnerConf = Map(
      "HOME" -> context.home,
      "OWNER" -> context.owner,
      "GROUP_ID" -> context.groupId,
      "directData" -> "true",
      "UDF_CLIENT" -> name
    ) ++ getSchemaAndConf(envSession) ++ configureLogConf

    val pythonVersion = runnerConf.getOrElse("pythonVersion", "3.6")


    val sourceSchema = params.get("sourceSchema") match {
      case Some(schemaStr) => schemaFromStr(schemaStr)
      case None => StructType(Array(StructField("value", ArrayType(DoubleType))))
    }

    val outputSchema = params.get("outputSchema") match {
      case Some(schemaStr) => schemaFromStr(schemaStr)
      case None => StructType(Array(StructField("value", ArrayType(ArrayType(DoubleType)))))
    }

    val debug = params.get("debugMode")

    sourceSchema match {
      case StructType(Array(StructField("value", StringType, _, _))) =>

        session.udf.register(name, (inputs: Seq[String]) => {
          val startTime = System.currentTimeMillis()
          val rows = inputs.map(input => Row.fromSeq(Seq(input)))
          val newRows = Ray.executePythonCode(predictCode, envs4j,
            rows.toIterator, sourceSchema, outputSchema, runnerConf, timezoneID, pythonVersion)
          val res = newRows.map(_.getAs[Seq[String]](0)).head
          if (debug.isDefined) {
            logInfo(s"Execute predict code time:${System.currentTimeMillis() - startTime}")
          }
          res

        })

      case StructType(Array(StructField("value", ArrayType(DoubleType, _), _, _))) =>
        session.udf.register(name, (vectors: Seq[Seq[Double]]) => {
          val startTime = System.currentTimeMillis()
          val rows = vectors.map(vector => Row.fromSeq(Seq(vector)))
          val newRows = Ray.executePythonCode(predictCode, envs4j,
            rows.toIterator, sourceSchema, outputSchema, runnerConf, timezoneID, pythonVersion)
          val res = newRows.map(_.getAs[Seq[Seq[Double]]](0)).head
          if (debug.isDefined) {
            logInfo(s"Execute predict code time:${System.currentTimeMillis() - startTime}")
          }
          res

        })
    }


    null
  }


  override def skipPathPrefix: Boolean = true
}

object Ray {
  def executePythonCode(code: String, envs: util.HashMap[String, String],
                        input: Iterator[Row], inputSchema: StructType,
                        outputSchema: StructType, conf: Map[String, String], timezoneID: String, pythonVersion: String): List[Row] = {
    import scala.collection.JavaConverters._

    val pythonExec = conf.getOrElse("pythonExec", "python")

    val batch = new ArrowPythonRunner(
      Seq(ChainedPythonFunctions(Seq(PythonFunction(code, envs, pythonExec, pythonVersion)))), inputSchema,
      timezoneID, conf
    )

    val sourceEnconder = WowRowEncoder.fromRow(inputSchema)
    val newIter = input.map { irow =>
      sourceEnconder(irow).copy()
    }

    // run the code and get the return result
    val javaConext = new JavaContext
    val commonTaskContext = new AppContextImpl(javaConext, batch)
    val columnarBatchIter = batch.compute(Iterator(newIter), TaskContext.getPartitionId(), commonTaskContext)

    val outputEnconder = WowRowEncoder.toRow(outputSchema)
    val items = columnarBatchIter.flatMap { batch =>
      batch.rowIterator.asScala
    }.map { r =>
      outputEnconder(r)
    }.toList
    javaConext.markComplete
    javaConext.close
    return items
  }
}





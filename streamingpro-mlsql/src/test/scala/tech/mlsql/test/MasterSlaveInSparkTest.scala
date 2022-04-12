package tech.mlsql.test

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.{TaskContext, WowRowEncoder}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.streaming.{BasicSparkOperation, SparkOperationUtil}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import tech.mlsql.arrow.ArrowUtils
import tech.mlsql.arrow.context.CommonTaskContext
import tech.mlsql.arrow.python.iapp.{AppContextImpl, JavaContext}
import tech.mlsql.arrow.python.ispark.SparkContextImp
import tech.mlsql.arrow.python.runner.{ArrowPythonRunner, ChainedPythonFunctions, PythonConf, PythonFunction, ReaderIterator, SparkSocketRunner, SpecialLengths}
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.ets.ray.DataServer
import tech.mlsql.nativelib.runtime.NativeFuncRule
import tech.mlsql.schema.parser.SparkSimpleSchemaParser
import tech.mlsql.session.SetSession
import tech.mlsql.tool.MasterSlaveInSpark

import java.io.{BufferedInputStream, DataInputStream, DataOutputStream, IOException, PrintWriter}
import java.net.Socket
import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.{asScalaBufferConverter, asScalaIteratorConverter}

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/4/7 15:03
 *
 */
class MasterSlaveInSparkTest extends FunSuite with SparkOperationUtil with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll {

  test("MasterSlaveInSpark") {
    def batchParams = Array(
      "-streaming.master", "local[4]",
      "-streaming.name", "unit-test",
      "-streaming.rest", "false",
      "-streaming.platform", "spark",
      "-streaming.enableHiveSupport", "true",
      "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
      "-streaming.spark.service", "false",
      "-streaming.unittest", "true"
    )


    withBatchContext(setupBatchContext(batchParams, null)) { runtime: SparkRuntime =>
      val session = runtime.sparkSession
      val modelDf = session.createDataFrame(Seq(
        ("ming", 20, 15552211521L),
        ("hong", 19, 13287994007L),
        ("zhi", 21, 15552211523L)
      )).toDF("name", "age", "phone")
      val runnerConf = Map("a" -> "A")
      val modelServer = new MasterSlaveInSpark("temp-model-server-in-spark", modelDf.sparkSession, "admin")
      modelServer.build(modelDf, MasterSlaveInSpark.defaultDataServerWithIterCountImpl, runnerConf)
      modelServer.waitWithTimeout(60)

      val (targetLen, sort) = modelServer.computeSplits(modelDf)
      assert(targetLen == modelServer.getRefs().get().length)

      import session.implicits._
      val timezoneID = session.sessionState.conf.sessionLocalTimeZone
      val newdf = session.createDataset[DataServer](modelServer.dataServers.get().map(f => DataServer(f.host, f.port, timezoneID))).repartition(1)
      val res = newdf.rdd.flatMap { row =>
        val socketRunner = new SparkSocketRunner("readFromStreamWithArrow", NetUtils.getHost, timezoneID)
        val host = row.host
        val port = row.port
        //        val worker = new Socket(host, port)
        //        val stream = new DataInputStream(worker.getInputStream)
        val commonTaskContext = new SparkContextImp(TaskContext.get(), null)
        val iter = socketRunner.readFromStreamWithArrow(host, port, commonTaskContext)
        iter.map(f => f.copy())
      }.collect()
      print(res.length)
    }
  }

  object RayCode {

    def pythonCode() =
      s"""
         |from pyjava.api.mlsql import PythonContext,RayContext
         |
         |context:PythonContext = context
         |
         |ray_context = RayContext.connect(globals(),None)
         |df = ray_context.to_dataset().to_dask()
         |c = df.shape[0].compute()
         |
         |context.build_result([{"size":c}])
         |
         |""".stripMargin

    def code() =
      s"""
         |
         |select 1 as a as mockTable;
         |
         |!python conf "maxIterCount=800";
         |!python conf "schema=st(field(size,long))";
         |!python env "PYTHON_ENV=source activate byzer";
         |!python conf "dataMode=model";
         |
         |run command as Ray.`` where
         |inputTable="mockTable"
         |and outputTable="newMockTable"
         |and code='''
         |
         |from pyjava.api.mlsql import PythonContext,RayContext
         |
         |context:PythonContext = context
         |
         |ray_context = RayContext.connect(globals(),None)
         |df = ray_context.to_dataset().to_dask()
         |c = df.shape[0].compute()
         |
         |context.build_result([{"size":c}])
         |
         |
         |''';
         |
         |
    """.stripMargin
  }

}

class Reader() {
  def read(): Unit = {

  }
}

package tech.mlsql.test

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.{TaskContext, WowRowEncoder}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
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
import tech.mlsql.common.utils.log.Logging
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
      val schema = StructType(Seq(StructField("name", StringType), StructField("age", IntegerType), StructField("phone", LongType)))
      val convert = WowRowEncoder.toRow(schema)
      val res = newdf.rdd.flatMap { row =>
        val host = row.host
        val port = row.port
        val iter = ArrowReaderUtils.readFromStreamWithArrow(host, port)
        iter.map(f => f.copy())
      }.collect()
      res.foreach { item =>
        println(convert(item))
      }
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

abstract class ReaderIteratorForTest[OUT](
                                           stream: DataInputStream,
                                           startTime: Long)
  extends Iterator[OUT] with Logging {

  private var nextObj: OUT = _
  private var eos = false

  override def hasNext: Boolean = nextObj != null || {
    if (!eos) {
      nextObj = read()
      if (nextObj == null) return false
      hasNext
    } else {
      false
    }
  }

  override def next(): OUT = {
    if (hasNext) {
      val obj = nextObj
      nextObj = null.asInstanceOf[OUT]
      obj
    } else {
      Iterator.empty.next()
    }
  }

  /**
   * Reads next object from the stream.
   * When the stream reaches end of data, needs to process the following sections,
   * and then returns null.
   */
  protected def read(): OUT
}

object ArrowReaderUtils {
  def readFromStreamWithArrow(host: String, port: Int) = {
    val socket = new Socket(host, port)
    val stream = new DataInputStream(socket.getInputStream)
    new ReaderIteratorForTest[ColumnarBatch](stream, System.currentTimeMillis()) {
      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader ", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _

      private var batchLoaded = true

      protected override def read(): ColumnarBatch = {

        if (reader != null && batchLoaded) {
          batchLoaded = reader.loadNextBatch()
          if (batchLoaded) {
            val batch = new ColumnarBatch(vectors)
            batch.setNumRows(root.getRowCount)
            batch
          } else {
            reader.close(false)
            allocator.close()
            // Reach end of stream. Call `read()` again to read control data.
            null
          }
        } else {
          reader = new ArrowStreamReader(stream, allocator)
          root = try {
            // in case we have a empty stream
            reader.getVectorSchemaRoot()
          } catch {
            case e: Exception =>
              batchLoaded = false
              reader.close(false)
              allocator.close()
              // Reach end of stream. Call `read()` again to read control data.
              null
          }
          if (root == null) {
            return null
          }
          schema = ArrowUtils.fromArrowSchema(root.getSchema())
          vectors = root.getFieldVectors().asScala.map { vector =>
            new ArrowColumnVector(vector)
          }.toArray[ColumnVector]
          read()
        }

      }
    }.flatMap { batch =>
      batch.rowIterator.asScala
    }
  }
}

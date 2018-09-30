package streaming.dsl.mmlib.algs

import java.nio.file.{Files, Paths}
import java.util.{ArrayList, HashMap, UUID, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray

import org.apache.spark.{APIDeployPythonRunnerEnv, TaskContext}
import org.apache.spark.api.python.WowPythonRunner
import org.apache.spark.ml.feature.PythonBatchPredictDataSchema
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.{MatrixType, VectorType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.util.{ExternalCommandRunner, MatrixSerDer, ObjPickle}
import streaming.core.strategy.platform.PlatformManager
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by fchen on 2018/8/22.
  */
class SQLBatchPythonAlg extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val pythonAlg = new SQLPythonAlg()
    val model = pythonAlg.load(spark, path, params)
    val (modelsTemp, metasTemp, trainParams, selectedFitParam) =
      model.asInstanceOf[(Seq[String], Seq[Map[String, String]], Map[String, String], Map[String, Any])]
    val batchSize = params.getOrElse("batchSize", "10").toInt
    val inputCol = params.getOrElse("inputCol", "")
    require(inputCol != null && inputCol != "", s"inputCol in ${getClass} module should be configed!")
    val batchPredictFun = params.getOrElse("predictFun", UUID.randomUUID().toString.replaceAll("-", ""))
    val predictLabelColumnName = params.getOrElse("predictCol", "predict_label")
    val predictTableName = params.getOrElse("predictTable", "")
    require(
      predictTableName != null && predictTableName != "",
      s"predictTable in ${getClass} module should be configed!"
    )

    val schema = PythonBatchPredictDataSchema.newSchema(df)

    val rdd = df.rdd.mapPartitions(it => {
      var list = List.empty[List[Row]]
      var tmpList = List.empty[Row]
      var batchCount = 0
      while (it.hasNext) {
        val e = it.next()
        if (batchCount == batchSize) {
          list +:= tmpList
          batchCount = 0
          tmpList = List.empty[Row]
        } else {
          tmpList +:= e
          batchCount += 1
        }
      }
      if (batchCount != batchSize) {
        list +:= tmpList
      }
      list.map(x => {
        Row.fromSeq(Seq(x, SQLBatchPythonAlg.createNewFeatures(x, inputCol)))
      }).iterator
    })

    val systemParam = mapParams("systemParam", params)
    val pythonPath = systemParam.getOrElse("pythonPath", "python")
    val pythonVer = systemParam.getOrElse("pythonVer", "2.7")
    val kafkaParam = mapParams("kafkaParam", trainParams)

    // load python script
    val userPythonScript = SQLPythonFunc.findPythonPredictScript(spark, params, "")

    val maps = new HashMap[String, JMap[String, _]]()
    val item = new HashMap[String, String]()
    item.put("funcPath", "/tmp/" + System.currentTimeMillis())
    maps.put("systemParam", item)
    maps.put("internalSystemParam", selectedFitParam.asJava)
    val taskDirectory = SQLPythonFunc.getLocalRunPath(UUID.randomUUID().toString)
    val res = ExternalCommandRunner.run(taskDirectory,Seq(pythonPath, userPythonScript.fileName),
      maps,
      MapType(StringType, MapType(StringType, StringType)),
      userPythonScript.fileContent,
      userPythonScript.fileName, modelPath = null, recordLog = SQLPythonFunc.recordAnyLog(kafkaParam)
    )
    res.foreach(f => f)
    val command = Files.readAllBytes(Paths.get(item.get("funcPath")))
    val runtimeParams = PlatformManager.getRuntime.params.asScala.toMap

    // registe batch predict python function

    val recordLog = SQLPythonFunc.recordAnyLog(Map[String, String]())
    val models = spark.sparkContext.broadcast(modelsTemp)
    val f = (m: Matrix, modelPath: String) => {
      val modelRow = InternalRow.fromSeq(Seq(SQLPythonFunc.getLocalTempModelPath(modelPath)))
      val trainParamsRow = InternalRow.fromSeq(Seq(ArrayBasedMapData(params)))
      val v_ser = ObjPickle.pickleInternalRow(Seq(MatrixSerDer.serialize(m)).toIterator, MatrixSerDer.matrixSchema())
      val v_ser2 = ObjPickle.pickleInternalRow(Seq(modelRow).toIterator, StructType(Seq(StructField("modelPath", StringType))))
      var v_ser3 = v_ser ++ v_ser2
      //      if (enableCopyTrainParamsToPython) {
      //        val v_ser4 = pickleInternalRow(Seq(trainParamsRow).toIterator, StructType(Seq(StructField("trainParams", MapType(StringType, StringType)))))
      //        v_ser3 = v_ser3 ++ v_ser4
      //      }

      //      if (TaskContext.get() == null) {
      //        APIDeployPythonRunnerEnv.setTaskContext(APIDeployPythonRunnerEnv.createTaskContext())
      //      }
      val iter = WowPythonRunner.run(
        pythonPath, pythonVer, command, v_ser3, TaskContext.get().partitionId(), Array(), runtimeParams, recordLog
      )
      val a = iter.next()
      val predictValue = MatrixSerDer.deserialize(ObjPickle.unpickle(a).asInstanceOf[java.util.ArrayList[Object]].get(0))
      predictValue
    }

    val f2 = (m: Matrix) => {
      models.value.map { modelPath =>
        f(m, modelPath)
      }.head
    }

    val func = UserDefinedFunction(f2, MatrixType, Some(Seq(MatrixType)))
    spark.udf.register(batchPredictFun, func)

    // temp batch predict column name
    val tmpPredColName = UUID.randomUUID().toString.replaceAll("-", "")
    val pdf = spark.createDataFrame(rdd, schema)
      .selectExpr(s"${batchPredictFun}(newFeature) as ${tmpPredColName}", "originalData")

    //    pdf.write.mode(SaveMode.Overwrite).json("/tmp/result")

    val prdd = pdf.rdd.mapPartitions(it => {
      var list = List.empty[Row]
      while (it.hasNext) {
        val e = it.next()
        val originalData = e.getAs[WrappedArray[Row]]("originalData")
        val newFeature = e.getAs[Matrix](tmpPredColName).rowIter.toList
        val size = originalData.size
        (0 until size).map(index => {
          val od = originalData(index)
          val pd = newFeature(index)
          list +:= Row.fromSeq(od.toSeq ++ Seq(pd))
        })
      }
      list.iterator
    })
    val pschema = df.schema.add(predictLabelColumnName, VectorType)
    //    spark.createDataFrame(prdd, pschema).write.mode(SaveMode.Overwrite).json("/tmp/fresult")
    spark.createDataFrame(prdd, pschema).createOrReplaceTempView(predictTableName)
    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new UnsupportedOperationException(s"${getClass.getSimpleName} unsupport load function")
  }

  override def predict(sparkSession: SparkSession,
                       _model: Any,
                       name: String,
                       params: Map[String, String]): UserDefinedFunction = {
    throw new UnsupportedOperationException(s"${getClass.getSimpleName} unsupport predict function")

  }
}

object SQLBatchPythonAlg {
  private def createNewFeatures(list: List[Row], inputCol: String): Matrix = {
    val numRows = list.size
    val numCols = list.head.getAs[Vector](inputCol).size
    val values = new ArrayList[Double](numCols * numRows)

    val vectorArray = list.map(r => {
      r.getAs[Vector](inputCol).toArray
    })
    for (i <- (0 until numCols)) {
      for (j <- (0 until numRows)) {
        values.add(vectorArray(j)(i))
      }
    }
    Matrices.dense(numRows, numCols, values.asScala.toArray).toSparse
  }
}


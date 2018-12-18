/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl.mmlib.algs.python

import java.nio.file.{Files, Paths}
import java.util.UUID

import org.apache.spark.{APIDeployPythonRunnerEnv, TaskContext}
import org.apache.spark.api.python.WowPythonRunner
import org.apache.spark.ml.feature.PythonBatchPredictDataSchema
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.linalg.SQLDataTypes.{MatrixType, VectorType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.util.{ExternalCommandRunner, MatrixSerDer, ObjPickle}
import streaming.core.strategy.platform.PlatformManager
import streaming.dsl.mmlib.algs.{Functions, SQLPythonAlg, SQLPythonFunc}
import scala.collection.JavaConverters._

import scala.collection.mutable

class APIBatchPredict(df: DataFrame, _path: String, params: Map[String, String]) extends Functions {
//  def predict: DataFrame = {
//    val sparkSession = df.sparkSession
//
//    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
//    val modelMeta = modelMetaManager.loadMetaAndModel
//
//    val batchSize = params.getOrElse("batchSize", "10").toInt
//    val inputCol = params.getOrElse("inputCol", "")
//
//    require(inputCol != null && inputCol != "", s"inputCol in ${getClass} module should be configured!")
//
//    val batchPredictFun = params.getOrElse("predictFun", UUID.randomUUID().toString.replaceAll("-", ""))
//    val predictLabelColumnName = params.getOrElse("predictCol", "predict_label")
//    val predictTableName = params.getOrElse("predictTable", "")
//
//    require(
//      predictTableName != null && predictTableName != "",
//      s"predictTable in ${getClass} module should be configured!"
//    )
//
//    val schema = PythonBatchPredictDataSchema.newSchema(df)
//
//    val rdd = df.rdd.mapPartitions(it => {
//      var list = List.empty[List[Row]]
//      var tmpList = List.empty[Row]
//      var batchCount = 0
//      while (it.hasNext) {
//        val e = it.next()
//        if (batchCount == batchSize) {
//          list +:= tmpList
//          batchCount = 0
//          tmpList = List.empty[Row]
//        } else {
//          tmpList +:= e
//          batchCount += 1
//        }
//      }
//      if (batchCount != batchSize) {
//        list +:= tmpList
//      }
//      list.map(x => {
//        Row.fromSeq(Seq(x, SQLPythonAlg.createNewFeatures(x, inputCol)))
//      }).iterator
//    })
//
//    val systemParam = mapParams("systemParam", modelMeta.trainParams)
//    val pythonPath = systemParam.getOrElse("pythonPath", "python")
//    val pythonVer = systemParam.getOrElse("pythonVer", "2.7")
//    val kafkaParam = mapParams("kafkaParam", modelMeta.trainParams)
//
//    // load python script
//    val userPythonScript = SQLPythonFunc.findPythonPredictScript(sparkSession, params, "")
//
//    val maps = new java.util.HashMap[String, java.util.Map[String, _]]()
//    val item = new java.util.HashMap[String, String]()
//    item.put("funcPath", "/tmp/" + System.currentTimeMillis())
//    maps.put("systemParam", item)
//    maps.put("internalSystemParam", modelMeta.resources.asJava)
//
//    val taskDirectory = SQLPythonFunc.getLocalRunPath(UUID.randomUUID().toString)
//
//    val res = ExternalCommandRunner.run(taskDirectory, Seq(pythonPath, userPythonScript.fileName),
//      maps,
//      MapType(StringType, MapType(StringType, StringType)),
//      userPythonScript.fileContent,
//      userPythonScript.fileName, modelPath = null, recordLog = SQLPythonFunc.recordAnyLog(kafkaParam)
//    )
//    res.foreach(f => f)
//    val command = Files.readAllBytes(Paths.get(item.get("funcPath")))
//    val runtimeParams = PlatformManager.getRuntime.params.asScala.toMap
//
//    // registe batch predict python function
//
//    val recordLog = SQLPythonFunc.recordAnyLog(Map[String, String]())
//    val models = sparkSession.sparkContext.broadcast(modelMeta.modelEntityPaths)
//    val f = (m: Matrix, modelPath: String) => {
//      val modelRow = InternalRow.fromSeq(Seq(SQLPythonFunc.getLocalTempModelPath(modelPath)))
//      val trainParamsRow = InternalRow.fromSeq(Seq(ArrayBasedMapData(params)))
//      val v_ser = ObjPickle.pickleInternalRow(Seq(MatrixSerDer.serialize(m)).toIterator, MatrixSerDer.matrixSchema())
//      val v_ser2 = ObjPickle.pickleInternalRow(Seq(modelRow).toIterator, StructType(Seq(StructField("modelPath", StringType))))
//      val v_ser3 = v_ser ++ v_ser2
//
//      if (TaskContext.get() == null) {
//        APIDeployPythonRunnerEnv.setTaskContext(APIDeployPythonRunnerEnv.createTaskContext())
//      }
//      val iter = WowPythonRunner.run(
//        pythonPath, pythonVer, command, v_ser3, TaskContext.get().partitionId(), Array(), runtimeParams, recordLog
//      )
//      val a = iter.next()
//      val predictValue = MatrixSerDer.deserialize(ObjPickle.unpickle(a).asInstanceOf[java.util.ArrayList[Object]].get(0))
//      predictValue
//    }
//
//    val f2 = (m: Matrix) => {
//      models.value.map { modelPath =>
//        f(m, modelPath)
//      }.head
//    }
//
//    val func = UserDefinedFunction(f2, MatrixType, Some(Seq(MatrixType)))
//    sparkSession.udf.register(batchPredictFun, func)
//
//    // temp batch predict column name
//    val tmpPredColName = UUID.randomUUID().toString.replaceAll("-", "")
//    val pdf = sparkSession.createDataFrame(rdd, schema)
//      .selectExpr(s"${batchPredictFun}(newFeature) as ${tmpPredColName}", "originalData")
//
//    val prdd = pdf.rdd.mapPartitions(it => {
//      var list = List.empty[Row]
//      while (it.hasNext) {
//        val e = it.next()
//        val originalData = e.getAs[mutable.WrappedArray[Row]]("originalData")
//        val newFeature = e.getAs[Matrix](tmpPredColName).rowIter.toList
//        val size = originalData.size
//        (0 until size).map(index => {
//          val od = originalData(index)
//          val pd = newFeature(index)
//          list +:= Row.fromSeq(od.toSeq ++ Seq(pd))
//        })
//      }
//      list.iterator
//    })
//    val pschema = df.schema.add(predictLabelColumnName, VectorType)
//    val newdf = sparkSession.createDataFrame(prdd, pschema)
//    newdf.createOrReplaceTempView(predictTableName)
//    newdf
//  }
}

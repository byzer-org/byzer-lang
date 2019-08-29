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

package streaming.dsl.mmlib.algs

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.ExternalCommandRunner
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.SQLPythonFunc._
import tech.mlsql.common.utils.hdfs.HDFSOperator

import scala.collection.JavaConverters._

/**
  * Created by allwefantasy on 5/2/2018.
  * This Module support training or predicting with user-defined python script
  */
class SQLPythonAlgBatchPrediction extends SQLAlg with Functions {
  override def train(df: DataFrame, wowPath: String, params: Map[String, String]): DataFrame = {

    val kafkaParam = mapParams("kafkaParam", params)

    require(kafkaParam.size > 0, "kafkaParam should be configured")

    val systemParam = mapParams("systemParam", params)
    val fitParam = mapParams("fitParam", params)

    require(fitParam.size > 0, "fitParam should be configured")

    val userPythonScript = loadUserDefinePythonScript(params, df.sparkSession)

    val schema = df.schema

    // load resource
    var resourceParams = Map.empty[String, String]
    if (fitParam.keys.map(_.split("\\.")(0)).toSet.contains("resource")) {
      val resources = Functions.mapParams(s"resource", fitParam)
      resources.foreach {
        case (resourceName, resourcePath) =>
          val tempResourceLocalPath = SQLPythonFunc.getLocalTempResourcePath(resourcePath, resourceName)
          recordSingleLineLog(kafkaParam, s"resource paramter found,system will load resource ${resourcePath} in ${tempResourceLocalPath} in executor.")
          HDFSOperator.copyToLocalFile(tempResourceLocalPath, resourcePath, true)
          resourceParams += (resourceName -> tempResourceLocalPath)
          recordSingleLineLog(kafkaParam, s"resource loaded.")
      }
    }


    val sessionLocalTimeZone = df.sparkSession.sessionState.conf.sessionLocalTimeZone
    val hdfsModelPath = fitParam("modelPath")

    require(!hdfsModelPath.contains(".."), "modelPath should not contains relative path")

    val wowRDD = df.rdd.mapPartitionsWithIndex { (algIndex, data) =>

      val pythonPath = systemParam.getOrElse("pythonPath", "python")
      val pythonVer = systemParam.getOrElse("pythonVer", "2.7")
      val pythonParam = systemParam.getOrElse("pythonParam", "").split(",").filterNot(f => f.isEmpty)


      val tempDataLocalPath = SQLPythonFunc.getLocalTempDataPath(wowPath)
      val tempModelLocalPath = s"${SQLPythonFunc.getLocalBasePath}/${UUID.randomUUID().toString}/${algIndex}"
      val tempResultLocalPath = s"${SQLPythonFunc.getLocalBasePath}/${UUID.randomUUID().toString}/${algIndex}"
      val resultHDFSPath = s"${wowPath}/data"

      val fs = FileSystem.get(new Configuration())
      fs.copyToLocalFile(new Path(hdfsModelPath),
        new Path(tempModelLocalPath))

      var tempDataLocalPathWithAlgSuffix = tempDataLocalPath
      tempDataLocalPathWithAlgSuffix = tempDataLocalPathWithAlgSuffix + "/" + algIndex
      FileUtils.forceMkdir(new File(tempDataLocalPathWithAlgSuffix))

      //here we write data to local
      val fileWriter = Files.newBufferedWriter(Paths.get(tempDataLocalPathWithAlgSuffix + "/0.json"), Charset.forName("utf-8"))
      try {
        WowJsonInferSchema.toJson(data, schema, sessionLocalTimeZone, json => {
          fileWriter.write(json)
          fileWriter.newLine()
        })
        fileWriter.flush()
      } finally {
        fileWriter.close()
      }


      val paramMap = new util.HashMap[String, Object]()
      val pythonScript = userPythonScript.get

      paramMap.put("fitParam", fitParam.asJava)

      val kafkaP = kafkaParam + ("group_id" -> (kafkaParam("group_id") + "_" + algIndex))
      paramMap.put("kafkaParam", kafkaP.asJava)

      val internalSystemParam = Map(
        "tempModelLocalPath" -> tempModelLocalPath,
        "tempDataLocalPath" -> tempDataLocalPathWithAlgSuffix,
        "tempResultLocalPath" -> tempResultLocalPath,
        "resource" -> resourceParams.asJava
      )

      paramMap.put("internalSystemParam", internalSystemParam.asJava)
      paramMap.put("systemParam", systemParam.asJava)


      val command = Seq(pythonPath) ++ pythonParam ++ Seq(pythonScript.fileName)

      val modelTrainStartTime = System.currentTimeMillis()

      var score = 0.0
      var trainFailFlag = false
      val taskDirectory = SQLPythonFunc.getLocalRunPath(UUID.randomUUID().toString)
      try {
        val res = ExternalCommandRunner.run(taskDirectory,
          command = command,
          iter = paramMap,
          schema = MapType(StringType, MapType(StringType, StringType)),
          scriptContent = pythonScript.fileContent,
          scriptName = pythonScript.fileName,
          recordLog = SQLPythonFunc.recordAnyLog(kafkaParam),
          modelPath = "", validateData = Array()
        )

        score = recordUserLog(algIndex, pythonScript, kafkaParam, res)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          trainFailFlag = true
      }

      try {
        //模型保存到hdfs上
        fs.delete(new Path(resultHDFSPath), true)
        fs.copyFromLocalFile(new Path(tempResultLocalPath),
          new Path(resultHDFSPath))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          trainFailFlag = true
      } finally {
        // delete local model
        FileUtils.deleteDirectory(new File(tempModelLocalPath))
        // delete local data
        FileUtils.deleteDirectory(new File(tempDataLocalPathWithAlgSuffix))
      }

      Seq().toIterator
    }
    wowRDD.count()
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }
}

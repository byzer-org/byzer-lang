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

import java.nio.charset.Charset

import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.hdfs.HDFSOperator

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-02-18 WilliamZhu(allwefantasy@gmail.com)
  * run command DownloadExt.`` where from="" and to=""
  */
class SQLUploadFileToServerExt(override val uid: String) extends SQLAlg with Functions with WowParams with WowLog {


  def evaluate(value: String) = {
    def withPathPrefix(prefix: String, path: String): String = {

      val newPath = path
      if (prefix.isEmpty) return newPath

      if (path.contains("..")) {
        throw new RuntimeException("path should not contains ..")
      }
      if (path.startsWith("/")) {
        return prefix + path.substring(1, path.length)
      }
      return prefix + newPath

    }

    val context = ScriptSQLExec.context()
    withPathPrefix(context.home, value)
  }

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession

    val context = ScriptSQLExec.context()
    val uploadUrl = context.userDefinedParam.get("__default__fileserver_upload_url__") match {
      case Some(fileServer) => fileServer
      case None =>
        require($(to).startsWith("http://"), "")
        $(to)
    }

    params.get(tokenName.name).map { f =>
      set(tokenName, f)
      f
    }.getOrElse {
      set(tokenName, "")
    }

    params.get(tokenValue.name).map { f =>
      set(tokenValue, f)
      f
    }.getOrElse {
      set(tokenValue, "")
    }

    def uploadFile(forUploadPath: String, fileName: String) = {
      val inputStream = HDFSOperator.readAsInputStream(forUploadPath)
      try {
        val entity = MultipartEntityBuilder.create.
          setMode(HttpMultipartMode.BROWSER_COMPATIBLE).
          setCharset(Charset.forName("utf-8")).
          addBinaryBody(fileName, inputStream, ContentType.MULTIPART_FORM_DATA, fileName).build
        logInfo(format(s"upload file ${forUploadPath} to ${uploadUrl}"))
        val downloadRes = Request.Post(uploadUrl).connectTimeout(60 * 1000)
          .socketTimeout(10 * 60 * 1000).addHeader($(tokenName), $(tokenValue)).body(entity)
          .execute().returnContent().asString()
        downloadRes
      } finally {
        inputStream.close()
      }


    }

    def findSubDirectory(path: String, targetDir: String) = {
      val pathChunks = path.split("/").filterNot(f => f.isEmpty)
      pathChunks.drop(pathChunks.indexOf(targetDir)).mkString("/")
    }

    val targetDir = path.split("/").filterNot(f => f.isEmpty).last

    var downloadResult = ArrayBuffer[UploadFileToServerRes]()
    if (HDFSOperator.isDir(path)) {
      val files = HDFSOperator.iteratorFiles(path, true)
      files.foreach { file =>
        downloadResult += UploadFileToServerRes(uploadFile(file, findSubDirectory(file, targetDir)), targetDir)
      }
    }

    if (HDFSOperator.isFile(path)) {
      downloadResult += UploadFileToServerRes(uploadFile(path, targetDir), targetDir)
    }

    import spark.implicits._
    spark.createDataset[UploadFileToServerRes](downloadResult).toDF()
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new RuntimeException("register is not support")


  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new RuntimeException("register is not support")

  final val to: Param[String] = new Param[String](this, "to", "the http url you want to upload file")
  final val tokenName: Param[String] = new Param[String](this, "tokenName", "the token upload server requires")
  final val tokenValue: Param[String] = new Param[String](this, "tokenValue", "the token upload server requires")


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

}

case class UploadFileToServerRes(response: String, dir: String)


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

import java.net.URLEncoder

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.http.client.fluent.Request
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.common.HDFSOperator
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-02-18 WilliamZhu(allwefantasy@gmail.com)
  * run command DownloadExt.`` where from="" and to=""
  */
class SQLDownloadExt(override val uid: String) extends SQLAlg with WowParams {


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
    params.get(from.name).map { s =>
      set(from, s)
      s
    }.getOrElse {
      throw new MLSQLException(s"${from.name} is required")
    }

    params.get(to.name).map { s =>
      set(to, evaluate(s))
      s
    }.getOrElse {
      throw new MLSQLException(s"${to.name} is required")
    }
    val context = ScriptSQLExec.context()
    val fromUrl = context.userDefinedParam.get("__default__fileserver_url__") match {
      case Some(fileServer) => fileServer
      case None =>
        require($(from).startsWith("http"), "")
        $(from)
    }

    val auth_secret = context.userDefinedParam("__auth_secret__")
    val stream = Request.Get(fromUrl + s"?userName=${URLEncoder.encode(context.owner, "utf-8")}&fileName=${URLEncoder.encode($(from), "utf-8")}&auth_secret=${URLEncoder.encode(auth_secret, "utf-8")}")
      .connectTimeout(60 * 1000)
      .socketTimeout(10 * 60 * 1000)
      .execute().returnContent().asStream()
    val tarIS = new TarArchiveInputStream(stream)
    var downloadResultRes = ArrayBuffer[DownloadResult]()
    try {
      var entry = tarIS.getNextEntry
      while (entry != null) {
        if (tarIS.canReadEntryData(entry)) {
          if (!entry.isDirectory) {
            val dir = entry.getName.split("/").filterNot(f => f.isEmpty).dropRight(1).mkString("/")
            downloadResultRes += DownloadResult($(to) + "/" + dir + "/" + entry.getName.split("/").last)
            HDFSOperator.saveStream($(to) + "/" + dir, entry.getName.split("/").last, tarIS)
          }
          entry = tarIS.getNextEntry
        }
      }
    } finally {
      tarIS.close()
      stream.close()
    }
    import spark.implicits._
    spark.createDataset[DownloadResult](downloadResultRes).toDF()
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new RuntimeException("register is not support")


  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new RuntimeException("register is not support")


  final val from: Param[String] = new Param[String](this, "from", "the file(directory) name you have uploaded")
  final val to: Param[String] = new Param[String](this, "to", "the path you want to save")


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

}

case class DownloadResult(hdfsPath: String)

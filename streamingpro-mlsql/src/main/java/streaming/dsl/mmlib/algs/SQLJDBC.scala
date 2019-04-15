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


import net.sf.json.JSONObject
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.datasource.JDBCUtils
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.{ConnectMeta, DBMappingKey}

import scala.collection.JavaConverters._


/**
  * Created by allwefantasy on 25/8/2018.
  */
class SQLJDBC(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  def executeInDriver(options: Map[String, String]) = {
    val driver = options("driver")
    val url = options("url")
    Class.forName(driver)
    val connection = java.sql.DriverManager.getConnection(url, options("user"), options("password"))
    try {
      // we suppose that there is only one create if
      val statements = options.filter(f =>"""driver\-statement\-[0-9]+""".r.findFirstMatchIn(f._1).nonEmpty).
        map(f => (f._1.split("-").last.toInt, f._2)).toSeq.sortBy(f => f._1).map(f => f._2).map { f =>
        logInfo(s"${getClass.getName} execute: ${f}")
        connection.prepareStatement(f)
      }

      statements.map { f =>
        f.execute()
        f
      }.map(_.close())
    } finally {
      if (connection != null)
        connection.close()
    }

  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    params.get(sqlMode.name).
      map(m => set(sqlMode, m)).getOrElse {
      // we should be compatible with preview version. 
      set(sqlMode, "ddl")
    }
    var _params = params
    if (path.contains(".")) {
      val Array(db, table) = path.split("\\.", 2)
      ConnectMeta.presentThenCall(DBMappingKey("jdbc", db), options => {
        options.foreach { item =>
          _params += (item._1 -> item._2)
        }
      })
    }


    $(sqlMode) match {
      case "ddl" =>
        executeInDriver(_params)
        emptyDataFrame()(df)
      case "query" =>
        val res = JDBCUtils.executeQueryInDriver(_params)
        val rdd = df.sparkSession.sparkContext.parallelize(res.map(item => JSONObject.fromObject(item.asJava).toString()))
        df.sparkSession.read.json(rdd)

    }


  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support predict function.")
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }


  override def skipPathPrefix: Boolean = true

  final val sqlMode: Param[String] = new Param[String](this, "sqlMode", "query/ddl default:ddl")
  final val driverStatement: Param[String] = new Param[String](this, "driver-statement-[group]", "DDL you wanna run")
}


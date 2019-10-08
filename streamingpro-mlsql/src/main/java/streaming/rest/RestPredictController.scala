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

package streaming.rest

import net.csdn.annotation.rest.{At, NoAction}
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method._
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec}

import scala.collection.JavaConversions._


/**
  * Created by allwefantasy on 20/4/2018.
  */
class RestPredictController extends ApplicationController {

  @At(path = Array("/model/predict"), types = Array(GET, POST))
  def modelPredict = {
    intercept()
    createContext
    val res = param("dataType", "vector") match {
      case "vector" => vec2vecPredict
      case "string" => string2vecPredict
      case "row" => row2vecPredict
    }
    render(200, res)
  }

  @NoAction
  def serverless() = {

  }

  def getSession = {
    if (paramAsBoolean("sessionPerUser", false)) {
      runtime.asInstanceOf[SparkRuntime].getSession(param("owner", "admin"))
    } else {
      runtime.asInstanceOf[SparkRuntime].sparkSession
    }
  }

  @At(path = Array("/compute"), types = Array(GET, POST))
  def compute = {
    intercept()
    val sparkSession = getSession
    val res = WowJsonInferSchema.toJson(sparkSession.sql(param("sql"))).mkString(",")

    render(200, res)
  }

  def createContext = {
    val userDefineParams = params.toMap.filter(f => f._1.startsWith("context.")).map(f => (f._1.substring("context.".length), f._2)).toMap
    ScriptSQLExec.setContext(new MLSQLExecuteContext(null, param("owner"), "", "", userDefineParams))
  }

  def getSQL = {
    if (hasParam("sql")) {
      param("sql", "").split("select").mkString("")
    } else if (hasParam("pipeline")) {
      param("pipeline", "").split(",").reverse.foldRight("feature") { (acc, o) =>
        s"${acc}(${o})"
      } + " as feature"
    } else throw new IllegalArgumentException("parameter sql or pipline is required")
  }

  def vec2vecPredict = {
    //dense or sparse
    val vectorType = param("vecType", "dense")
    val sparkSession = getSession
    val vectors = JSONArray.fromObject(param("data", "[]")).map { f =>

      val vec = vectorType match {
        case "dense" =>
          val v = f.asInstanceOf[JSONArray].map(f => f.asInstanceOf[Number].doubleValue()).toArray
          Vectors.dense(v)
        case "sparse" =>
          val v = f.asInstanceOf[JSONObject].map(f => (f._1.asInstanceOf[Int], f._2.asInstanceOf[Number].doubleValue())).toMap
          require(paramAsInt("vecSize", -1) != -1, "when vector type is sparse, vecSize is required")
          Vectors.sparse(paramAsInt("vecSize", -1), v.keys.toArray, v.values.toArray)
      }
      Feature(feature = vec)
    }
    import sparkSession.implicits._
    //select vec_argmax(tf_predict(feature,"feature","label",2)) as predict_label
    val sql = getSQL
    val res = sparkSession.createDataset(sparkSession.sparkContext.parallelize(vectors)).selectExpr(sql).toJSON.collect().mkString(",")
    "[" + res + "]"


  }

  def row2vecPredict = {
    val sparkSession = getSession
    val strList = JSONArray.fromObject(param("data", "[]")).map(f => f.toString)
    val sql = getSQL
    val perRequestCoreNum = paramAsInt("perRequestCoreNum", 1)
    //import sparkSession.implicits._
    //val rdd = sparkSession.sparkContext.parallelize(strList, perRequestCoreNum)
    val res = WowJsonInferSchema.toJson(WowJsonInferSchema.createDataSet(strList, sparkSession).selectExpr(sql)).mkString(",")
    "[" + res + "]"

  }

  def string2vecPredict = {
    val sparkSession = getSession
    val strList = JSONArray.fromObject(param("data", "[]")).map(f => StringFeature(f.toString))
    val sql = getSQL
    val perRequestCoreNum = paramAsInt("perRequestCoreNum", 1)
    import sparkSession.implicits._
    val res = WowJsonInferSchema.toJson(sparkSession.createDataset(strList).selectExpr(sql)).mkString(",")
    "[" + res + "]"

  }

  def runtime = PlatformManager.getRuntime

  def intercept() = {
    val jparams = runtime.asInstanceOf[SparkRuntime].params
    if (jparams.containsKey("streaming.rest.intercept.clzz")) {
      val interceptor = Class.forName(jparams("streaming.rest.intercept.clzz").toString).newInstance()
      interceptor.asInstanceOf[RestInterceptor].before(request = request.httpServletRequest(), response = restResponse.httpServletResponse())
    }
  }
}


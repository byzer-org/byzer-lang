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

package streaming.core.compositor.spark.streaming.output

import java.util
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.streaming.dstream.DStream
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper
import streaming.core.compositor.spark.helper.MultiSQLOutputHelper
import streaming.core.strategy.ParamsValidator

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class MultiSQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[MultiSQLOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstreams = middleResult.get(0).asInstanceOf[ArrayBuffer[(DStream[(String, String)], String)]]

    val funcs = if (params.containsKey("sqlList")) {
      params.get("sqlList").asInstanceOf[ArrayBuffer[() => Unit]]
    } else ArrayBuffer[() => Unit]()

    val spark = sparkSession(params)

    def output() = {
      _configParams.foreach { config =>

        val name = config.getOrElse("name", "").toString
        val _cfg = config.map(f => (f._1.toString, f._2.toString)).map { f =>
          (f._1, params.getOrElse(s"streaming.sql.out.${name}.${f._1}", f._2).toString)
        }.toMap

        val _outputWriterClzz = _cfg.get("clzz")

        _outputWriterClzz match {
          case Some(clzz) =>
            import streaming.core.compositor.spark.api.OutputWriter
            Class.forName(clzz).newInstance().
              asInstanceOf[OutputWriter].write(spark.sqlContext, params.toMap, _cfg)
          case None =>
            MultiSQLOutputHelper.output(_cfg, sparkSession(params))
        }
      }
    }

    dstreams.foreach { dstreamWithName =>
      val name = dstreamWithName._2
      val dstream = dstreamWithName._1
      dstream.foreachRDD { rdd =>
        import spark.implicits._
        val df = rdd.toDF("key", "value")
        df.createOrReplaceTempView(name)
        funcs.foreach { f =>
          try {
            f()
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }
        }
        output()
      }
    }

    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }
}

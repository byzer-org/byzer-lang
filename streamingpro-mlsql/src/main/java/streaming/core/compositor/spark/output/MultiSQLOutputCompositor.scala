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

package streaming.core.compositor.spark.output

import java.util

import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import _root_.streaming.core.CompositorHelper
import _root_.streaming.core.strategy.ParamsValidator
import org.apache.spark.sql.{DataFrame, SaveMode}
import streaming.core.compositor.spark.helper.MultiSQLOutputHelper

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 30/3/2017.
  */
class MultiSQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[MultiSQLOutputCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    _configParams.foreach { config =>

      val name = config.getOrElse("name", "").toString
      val _cfg = config.map(f => (f._1.toString, f._2.toString)).map { f =>
        (f._1, params.getOrElse(s"streaming.sql.out.${name}.${f._1}", f._2).toString)
      }.toMap

      val _outputWriterClzz = _cfg.get("clzz")

      _outputWriterClzz match {

        case Some(clzz) =>
          import streaming.core.compositor.spark.api.OutputWriter
          Class.forName(clzz).
            newInstance().asInstanceOf[OutputWriter].write(sparkSession(params).sqlContext, params.toMap, _cfg)

        case None =>

          MultiSQLOutputHelper.output(_cfg, sparkSession(params))
      }


    }

    val cacheKey = "__caches__"
    if (params.containsKey(cacheKey)) {
      params.get(cacheKey).asInstanceOf[util.List[DataFrame]].foreach { df =>
        df.unpersist()
      }
    }

    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }
}

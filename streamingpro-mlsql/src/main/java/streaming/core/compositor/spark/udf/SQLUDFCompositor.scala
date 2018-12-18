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

package streaming.core.compositor.spark.udf

import java.lang.reflect.Modifier
import java.util

import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper

import scala.collection.JavaConversions._

/**
  * 7/4/16 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLUDFCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[SQLUDFCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val sc = (params)
    val sqlContext = sparkSession(params)
    _configParams(0).foreach { f =>
      val objMethod = Class.forName(f._2.toString)
      objMethod.getMethods.foreach { f =>
        try {
          if (Modifier.isStatic(f.getModifiers)) {
            f.invoke(null, sqlContext.udf)
          }
        } catch {
          case e: Exception =>
            logger.info(s"${f.getName} missing",e)
        }
      }
    }
    middleResult
  }
}


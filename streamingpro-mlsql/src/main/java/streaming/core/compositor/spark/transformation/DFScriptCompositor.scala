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

package streaming.core.compositor.spark.transformation

import java.util

import net.liftweb.{json => SJSon}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.util.{ScalaSourceCodeCompiler, ScriptCacheKey}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper
import streaming.core.compositor.spark.api.Transform

import scala.collection.JavaConversions._

/**
  * 8/2/16 WilliamZhu(allwefantasy@gmail.com)
  */
class DFScriptCompositor[T] extends Compositor[T] with CompositorHelper {


  def scripts = {
    _configParams.get(1).map { fieldAndCode =>
      (fieldAndCode._1.toString, fieldAndCode._2 match {
        case a: util.List[String] => a.mkString(" ")
        case a: String => a
        case _ => ""
      })
    }
  }

  protected var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def source = {
    config[String]("source", _configParams)
  }

  def script = {
    config[String]("script", _configParams)
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    val context = sparkSession(params)
    val _source = source.getOrElse("")
    val _script = script.getOrElse("")
    val _transformClzz = config[String]("clzz", _configParams)

    def loadScriptFromFile(script: String) = {
      if ("file" == _source || script.startsWith("file:/") || script.startsWith("hdfs:/")) {
        context.sparkContext.textFile(script).collect().mkString("\n")
      } else if (script.startsWith("classpath:/")) {
        val cleanScriptFilePath = script.substring("classpath://".length)
        scala.io.Source.fromInputStream(
          this.getClass.getResourceAsStream(cleanScriptFilePath)).getLines().
          mkString("\n")
      }
      else script
    }

    _transformClzz match {
      case Some(clzz) =>
        Class.forName(clzz).newInstance().asInstanceOf[Transform].
          process(context.sqlContext, params.toMap, _configParams.get(0).map(f => (f._1.toString, f._2.toString)).toMap)
      case None =>
        val executor = ScalaSourceCodeCompiler.execute(ScriptCacheKey("context", loadScriptFromFile(_script)))
        executor.execute(context.sqlContext)
    }
    middleResult

  }

}

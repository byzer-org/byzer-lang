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

package streaming.udf

import com.google.common.cache.{CacheBuilder, CacheLoader}
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey
import streaming.log.Logging

import scala.collection.mutable.HashMap

/**
  * Created by fchen on 2018/11/13.
  */
trait RuntimeCompileScriptInterface[FunType] extends Logging {

  /**
    * compile source code or get binary code for cache.
    *
    * @param scriptCacheKey
    * @return
    */
  def driverExecute(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
    RuntimeCompileScriptFactory.driverScriptCache.get(scriptCacheKey)
  }

  def executorExecute(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
    RuntimeCompileScriptFactory.executorScriptCache.get(scriptCacheKey)
  }

  /**
    * validate the source code
    */
  def check(sourceCode: String): Boolean

  /**
    * how to compile the language source code with jvm.
    *
    * @param scriptCacheKey
    * @return
    */
  def compile(scriptCacheKey: ScriptUDFCacheKey): AnyRef

  /**
    * generate udf or udaf
    */
  def generateFunction(scriptCacheKey: ScriptUDFCacheKey): FunType

  def lang: String

}

object RuntimeCompileScriptFactory extends Logging {

  private val _udfCache = HashMap[String, RuntimeCompileUDF]()
  private val _udafCache = HashMap[String, RuntimeCompileUDAF]()

  registerUDF(PythonRuntimeCompileUDF.lang, PythonRuntimeCompileUDF)
  registerUDF(ScalaRuntimeCompileUDF.lang, ScalaRuntimeCompileUDF)
  registerUDAF(ScalaRuntimeCompileUDAF.lang, ScalaRuntimeCompileUDAF)
  registerUDAF(PythonRuntimeCompileUDAF.lang, PythonRuntimeCompileUDAF)

  def getUDFCompilerBylang(lang: String): Option[RuntimeCompileUDF] = {
    _udfCache.get(lang)
  }

  def getUDAFCompilerBylang(lang: String): Option[RuntimeCompileUDAF] = {
    _udafCache.get(lang)
  }

  def registerUDF(lang: String, runtimeCompileUDF: RuntimeCompileUDF): Unit = {
    logInfo(s"register $lang runtime compile udf" +
      s" engine ${runtimeCompileUDF.getClass.getCanonicalName}!")
    _udfCache.put(lang, runtimeCompileUDF)
  }

  def registerUDAF(lang: String, runtimeCompileUDAF: RuntimeCompileUDAF): Unit = {
    logInfo(s"register $lang runtime compile udaf" +
      s" engine ${runtimeCompileUDAF.getClass.getCanonicalName}!")
    _udafCache.put(lang, runtimeCompileUDAF)
  }


  val driverScriptCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build(
      new CacheLoader[ScriptUDFCacheKey, AnyRef]() {
        override def load(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
          val startTime = System.nanoTime()
          val compiler = scriptCacheKey.udfType match {
            case "udf" => getUDFCompilerBylang(scriptCacheKey.lang)
            case "udaf" => getUDAFCompilerBylang(scriptCacheKey.lang)
          }

          val compiled = compiler.get.compile(scriptCacheKey)

          def timeMs: Double = (System.nanoTime() - startTime).toDouble / 1000000

          logInfo(s"Dynamic in driver generate udf time: [ ${timeMs} ]ms.")
          compiled
        }
      })


  val executorScriptCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build(
      new CacheLoader[ScriptUDFCacheKey, AnyRef]() {
        override def load(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
          val startTime = System.nanoTime()
          val compiler = scriptCacheKey.udfType match {
            case "udf" => getUDFCompilerBylang(scriptCacheKey.lang)
            case "udaf" => getUDAFCompilerBylang(scriptCacheKey.lang)
          }

          val compiled = compiler.get.compile(scriptCacheKey)

          def timeMs: Double = (System.nanoTime() - startTime).toDouble / 1000000

          logInfo(s"Dynamic in executor generate udf time: [ ${timeMs} ]ms.")
          compiled
        }
      })

}

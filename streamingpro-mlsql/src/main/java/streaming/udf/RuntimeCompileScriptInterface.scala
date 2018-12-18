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

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.reflect.ClassPath
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey
import streaming.log.Logging

/**
 * Created by fchen on 2018/11/13.
 */
trait RuntimeCompileScriptInterface[FunType] extends Logging {

  private val _scriptCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build(
      new CacheLoader[ScriptUDFCacheKey, AnyRef]() {
        override def load(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
          val startTime = System.nanoTime()
          val compiled = compile(scriptCacheKey)

          def timeMs: Double = (System.nanoTime() - startTime).toDouble / 1000000

          logInfo(s"generate udf time: [ ${timeMs} ]ms.")
          compiled
        }
      })


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

  /**
   * compile source code or get binary code for cache.
   * @param scriptCacheKey
   * @return
   */
  def execute(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
    _scriptCache.get(scriptCacheKey)
  }
}

object RuntimeCompileScriptFactory extends Logging {

  private val _udfCache = HashMap[String, RuntimeCompileUDF]()
  private val _udafCache = HashMap[String, RuntimeCompileUDAF]()
  private val _loaded = new AtomicBoolean(false)
  private val _lock = new Object()

  def getUDFCompilerBylang(lang: String): Option[RuntimeCompileUDF] = {
    if (!_loaded.get()) {
      loadAll()
    }
    _udfCache.get(lang)
  }

  def getUDAFCompilerBylang(lang: String): Option[RuntimeCompileUDAF] = {
    if (!_loaded.get()) {
      loadAll()
    }
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

  /**
   * load all [[RuntimeCompileUDF]] and [[RuntimeCompileUDAF]]
   */
  def loadAll(): Unit = {
    def isRuntimeComile(className: String): Boolean = {
      (className.endsWith("RuntimeCompileUDF") && className != "streaming.udf.RuntimeCompileUDF") ||
        (className.endsWith("RuntimeCompileUDAF") && className != "streaming.udf.RuntimeCompileUDAF")
    }
    _lock.synchronized {
      ClassPath.from(this.getClass.getClassLoader)
        .getTopLevelClasses("streaming.udf")
        .map(_.load())
          .filter(n => {
            isRuntimeComile(n.getName)
          }).map(getInstance)
        .foreach {
          case udf: RuntimeCompileUDF => registerUDF(udf.lang, udf)
          case udaf: RuntimeCompileUDAF => registerUDAF(udaf.lang, udaf)
        }
      _loaded.set(true)
    }
  }

  private def getInstance(clz: Class[_]): Any = {
    import scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(this.getClass.getClassLoader)
    val module = runtimeMirror.staticModule(clz.getName)
    val obj = runtimeMirror.reflectModule(module)
    obj.instance
  }

}

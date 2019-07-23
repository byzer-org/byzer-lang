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

import org.apache.spark.sql.types.DataType
import org.python.core.{PyFunction, PyMethod, PyObject, PyTableCode}
import streaming.common.Md5
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey
import streaming.jython.{JythonUtils, PythonInterp}
import streaming.log.Logging
import tech.mlsql.schema.parser.SparkSimpleSchemaParser

/**
  * Created by fchen on 2018/11/14.
  */
object PythonRuntimeCompileUDF extends RuntimeCompileUDF with Logging {


  override def returnType(scriptCacheKey: ScriptUDFCacheKey): Option[DataType] = {
    Option(SparkSimpleSchemaParser.parse(scriptCacheKey.dataType))
  }


  /**
    * reture udf input argument number
    */
  override def argumentNum(scriptCacheKey: ScriptUDFCacheKey): Int = {

    val po = driverExecute(scriptCacheKey).asInstanceOf[PyObject]
    val pi = po.__getattr__(scriptCacheKey.methodName).asInstanceOf[PyMethod]
    pi.__func__.asInstanceOf[PyFunction].__code__.asInstanceOf[PyTableCode].co_argcount - 1
  }

  override def wrapCode(scriptCacheKey: ScriptUDFCacheKey): ScriptUDFCacheKey = {
    if (scriptCacheKey.className.isEmpty) {
      val (className, code) = wrapClass(scriptCacheKey.originalCode)
      scriptCacheKey.copy(wrappedCode = code, className = className)
    } else {
      scriptCacheKey.copy(wrappedCode = scriptCacheKey.originalCode)
    }
  }

  /**
    * validate the source code
    */
  override def check(sourceCode: String): Boolean = {
    true
  }

  /**
    * compile the source code.
    *
    * @param scriptCacheKey
    * @return
    */
  override def compile(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
    PythonInterp.compilePython(scriptCacheKey.wrappedCode, scriptCacheKey.className)
  }

  override def invokeFunctionFromInstance(scriptCacheKey: ScriptUDFCacheKey)
  : (Seq[Object]) => AnyRef = {

    val c = ScriptSQLExec.contextGetOrForTest()

    val wrap = (fn: () => Any) => {
      try {
        ScriptSQLExec.setContextIfNotPresent(c)
        fn()
      } catch {
        case e: Exception =>
          throw e
      }
    }

    // instance will call by spark executor, so we declare as lazy val
    lazy val instance = wrap(() => {
      executorExecute(scriptCacheKey).asInstanceOf[PyObject].__call__()
    }).asInstanceOf[PyObject]

    // the same with instance, method will call by spark executor too.
    lazy val method = instance.__getattr__(scriptCacheKey.methodName)

    val invokeFunc: (Seq[Any]) => AnyRef = {
      (args: Seq[Any]) => {
        val argsArray = args.map(JythonUtils.toPy).toArray
        JythonUtils.toJava(method.__call__(argsArray))
      }
    }
    invokeFunc
  }

  override def lang: String = "python"

  private def wrapClass(function: String): WrappedType = {
    val classNameHash = Md5.md5Hash(function)
    val temp = function.split("\n").map(f => s"    $f").mkString("\n")
    val className = s"StreamingProUDF_${classNameHash}"
    val newfun =
      s"""
         |# -*- coding: utf-8 -*-
         |class  ${className}:
         |${temp}
         """.stripMargin
    (className, newfun)
  }
}

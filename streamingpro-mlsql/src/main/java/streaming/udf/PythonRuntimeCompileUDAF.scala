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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import org.python.core.{Py, PyObject}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey
import streaming.jython.{JythonUtils, PythonInterp}

/**
  * Created by fchen on 2018/11/15.
  */
object PythonRuntimeCompileUDAF extends RuntimeCompileUDAF {
  /**
    * validate the source code
    */
  override def check(sourceCode: String): Boolean = true

  /**
    * compile the source code.
    *
    * @param scriptCacheKey
    * @return
    */
  override def compile(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
    PythonInterp.compilePython(scriptCacheKey.originalCode, scriptCacheKey.className)
  }


  override def generateFunction(scriptCacheKey: ScriptUDFCacheKey): UserDefinedAggregateFunction = {

    new UserDefinedAggregateFunction with Serializable {

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

      @transient val objectUsingInDriver = wrap(() => {
        driverExecute(scriptCacheKey).asInstanceOf[PyObject].__call__()
      }).asInstanceOf[PyObject]

      lazy val objectUsingInExecutor = wrap(() => {
        executorExecute(scriptCacheKey).asInstanceOf[PyObject].__call__()
      }).asInstanceOf[PyObject]


      val _inputSchema = objectUsingInDriver.__getattr__("inputSchema").__call__()
      val _dataType = objectUsingInDriver.__getattr__("dataType").__call__()
      val _bufferSchema = objectUsingInDriver.__getattr__("bufferSchema").__call__()
      val _deterministic = objectUsingInDriver.__getattr__("deterministic").__call__()

      override def inputSchema: StructType = {
        wrap(() => {
          _inputSchema.__tojava__(classOf[StructType]).asInstanceOf[StructType]
        }).asInstanceOf[StructType]
      }

      override def dataType: DataType = {
        wrap(() => {
          _dataType.__tojava__(classOf[DataType]).asInstanceOf[DataType]
        }).asInstanceOf[DataType]
      }

      override def bufferSchema: StructType = {
        wrap(() => {
          _bufferSchema.__tojava__(classOf[StructType]).asInstanceOf[StructType]
        }).asInstanceOf[StructType]

      }

      override def deterministic: Boolean = {
        wrap(() => {
          JythonUtils.toJava(_deterministic).toString.toInt match {
            case 0 => false
            case 1 => true
          }
        }).asInstanceOf[Boolean]

      }

      lazy val _update = objectUsingInExecutor.__getattr__("update")
      lazy val _merge = objectUsingInExecutor.__getattr__("merge")
      lazy val _initialize = objectUsingInExecutor.__getattr__("initialize")
      lazy val _evaluate = objectUsingInExecutor.__getattr__("evaluate")

      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        wrap(() => {
          _update.__call__(Py.java2py(buffer), Py.java2py(input))
        })

      }

      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        wrap(() => {
          _merge.__call__(Py.java2py(buffer1), Py.java2py(buffer2))
        })

      }

      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        wrap(() => {
          _initialize.__call__(Py.java2py(buffer))
        })

      }

      override def evaluate(buffer: Row): Any = {
        wrap(() => {
          JythonUtils.toJava(_evaluate.__call__(Py.java2py(buffer)))
        })

      }


    }
  }

  override def lang: String = "python"
}

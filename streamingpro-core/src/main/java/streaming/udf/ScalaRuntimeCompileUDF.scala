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

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey
import tech.mlsql.common.utils.Md5
import tech.mlsql.common.utils.lang.sc.SourceCodeCompiler
import tech.mlsql.common.utils.log.Logging

import scala.reflect.runtime.universe._
import scala.tools.reflect.{ToolBox, ToolBoxError}

/**
  * Created by fchen on 2018/11/14.
  */
object ScalaRuntimeCompileUDF extends RuntimeCompileUDF with ScalaCompileUtils with Logging {

  override def returnType(scriptCacheKey: ScriptUDFCacheKey): Option[DataType] = {

    getFunctionDef(scriptCacheKey)
      .map(defDef => {
        ScalaReflection.schemaFor(defDef.tpt.tpe).dataType
      })
  }

  override def argumentNum(scriptCacheKey: ScriptUDFCacheKey): Int = {
    val funcDef = getFunctionDef(scriptCacheKey)
    require(funcDef.isDefined, s"function ${scriptCacheKey.methodName} not found" +
      s" in ${scriptCacheKey.originalCode}")
    funcDef.get.vparamss.head.size
  }

  /**
    * validate the source code
    */
  override def check(sourceCode: String): Boolean = {
    var checkResult = false
    try {
      val tree = tb.parse(sourceCode)
      val typeCheckResult = tb.typecheck(tree)
      checkResult = typeCheckResult.isInstanceOf[DefDef] || typeCheckResult.isInstanceOf[ClassDef]
    }
    catch {
      // tb.parse and tb.typecheck could throw ToolBoxError, which is a sub-class of Throwable
      case err: ToolBoxError =>
        logError("ToolBoxError: ", err )
        throw new IllegalArgumentException("UDF Compilation error: " + err.message)
    }
    if (!checkResult) {
      throw new IllegalArgumentException(s"${sourceCode} isn't a function or class definition.")
    }
    checkResult
  }

  /**
    * compile the source code.
    *
    * @param scriptCacheKey
    * @return
    */
  override def compile(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
    val tree = tb.parse(prepareScala(scriptCacheKey.wrappedCode, scriptCacheKey.className))
    tb.compile(tree).apply().asInstanceOf[Class[_]]
  }

  override def lang: String = "scala"

  override def wrapCode(scriptCacheKey: ScriptUDFCacheKey): ScriptUDFCacheKey = {
    check(scriptCacheKey.originalCode)
    val tree = tb.parse(scriptCacheKey.originalCode)
    tb.typecheck(tree) match {
      case dd: DefDef =>
        val (className, code) = wrapClass(scriptCacheKey.originalCode)
        scriptCacheKey.copy(wrappedCode = code, className = className)
      case cd: ClassDef =>
        scriptCacheKey.copy(wrappedCode = scriptCacheKey.originalCode)
      case s: Any =>
        // never happen
        throw new IllegalArgumentException(s"script type ${s.getClass} isn't a function or class.")
    }
  }

  private def getFunctionDef(scriptCacheKey: ScriptUDFCacheKey): Option[DefDef] = {
    val tree = tb.parse(scriptCacheKey.wrappedCode)
    val classDef = tb.typecheck(tree).asInstanceOf[ClassDef]
    classDef.children
      .head
      .children
      .filter(_.isInstanceOf[DefDef])
      .map(_.asInstanceOf[DefDef])
      .filter(_.name.decodedName.toString == scriptCacheKey.methodName)
      .headOption
  }

  private def wrapClass(function: String): WrappedType = {
    val classNameHash = Md5.md5Hash(function)
    val className = s"StreamingProUDF_${classNameHash}"
    val newfun =
      s"""
         |class ${className} {
         |
         |${function}
         |
         |}
            """.stripMargin
    (className, newfun)
  }

  def invokeFunctionFromInstance(scriptCacheKey: ScriptUDFCacheKey): (Seq[Object]) => AnyRef = {

    lazy val clz = ScalaRuntimeCompileUDF.executorExecute(scriptCacheKey).asInstanceOf[Class[_]]
    lazy val instance = ScalaRuntimeCompileUDF.newInstance(clz)
    lazy val method = SourceCodeCompiler.getMethod(clz, scriptCacheKey.methodName)

    val func: (Seq[Object]) => AnyRef = {
      (args: Seq[Object]) => method.invoke(instance, args: _*)
    }
    func
  }
}

trait ScalaCompileUtils {
  var classLoader = Thread.currentThread().getContextClassLoader
  if (classLoader == null) {
    classLoader = scala.reflect.runtime.universe.getClass.getClassLoader
  }
  val tb = runtimeMirror(classLoader).mkToolBox()

  def prepareScala(src: String, className: String): String = {
    src + "\n" + s"scala.reflect.classTag[$className].runtimeClass"
  }

  def newInstance(clz: Class[_]): Any = {
    SourceCodeCompiler.newInstance(clz)
  }

}




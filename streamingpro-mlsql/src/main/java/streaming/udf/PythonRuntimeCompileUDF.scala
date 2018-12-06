package streaming.udf

import java.util.UUID

import org.apache.spark.sql.types.DataType
import org.python.core.{PyFunction, PyMethod, PyObject, PyTableCode}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey
import streaming.jython.{JythonUtils, PythonInterp}
import streaming.log.Logging
import streaming.parser.SparkTypePaser

/**
 * Created by fchen on 2018/11/14.
 */
object PythonRuntimeCompileUDF extends RuntimeCompileUDF with Logging {

  override def returnType(scriptCacheKey: ScriptUDFCacheKey): Option[DataType] = {
    Option(SparkTypePaser.toSparkType(scriptCacheKey.dataType))
  }

  /**
   * reture udf input argument number
   */
  override def argumentNum(scriptCacheKey: ScriptUDFCacheKey): Int = {

    val po = execute(scriptCacheKey).asInstanceOf[PyObject]
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
      execute(scriptCacheKey).asInstanceOf[PyObject].__call__()
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

    val temp = function.split("\n").map(f => s"    $f").mkString("\n")
    val className = s"StreamingProUDF_${UUID.randomUUID().toString.replaceAll("-", "")}"
    val newfun =
      s"""
         |# -*- coding: utf-8 -*-
         |class  ${className}:
         |${temp}
         """.stripMargin
    (className, newfun)
  }
}

package streaming.udf

import org.apache.spark.sql.MLSQLUtils
import org.apache.spark.sql.types.DataType
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey
import tech.mlsql.common.utils.Md5
import tech.mlsql.common.utils.lang.sc.SourceCodeCompiler
import tech.mlsql.common.utils.log.Logging

/**
  * Created by fchen on 2018/11/15.
  */
object JavaRuntimeCompileUDF extends RuntimeCompileUDF with Logging {
  /**
    * udf reture DataType
    * due to java type erasure, it's not good idea get function return type by `method.getReturnType`,
    * a batter idea is find return type form source code.
    */
  override def returnType(scriptCacheKey: ScriptUDFCacheKey): Option[DataType] = {
    val clazz = driverExecute(scriptCacheKey).asInstanceOf[Class[_]]
    val method = SourceCodeCompiler.getMethod(clazz, scriptCacheKey.methodName)
    Option(MLSQLUtils.getJavaDataType(method.getGenericReturnType)._1)
  }

  /**
    * reture udf input argument number
    */
  override def argumentNum(scriptCacheKey: ScriptUDFCacheKey): Int = {
    val clazz = driverExecute(scriptCacheKey).asInstanceOf[Class[_]]
    val method = SourceCodeCompiler.getMethod(clazz, scriptCacheKey.methodName)
    method.getParameterCount
  }

  /**
    * wrap original source code.
    * e.g. in [[ScalaRuntimCompileUDAF]], user pass function code, we should wrap code as a class.
    * so the runtime compiler will compile source code as runtime instance.
    */
  override def wrapCode(scriptCacheKey: ScriptUDFCacheKey): ScriptUDFCacheKey = {

    val className = if (scriptCacheKey.className == null || scriptCacheKey.className.isEmpty) {
      "UDF"
    } else {
      scriptCacheKey.className
    }
    val codeMd5 = Md5.md5Hash(scriptCacheKey.originalCode)
    val packageName = s"streaming.udf.java.sun${codeMd5}"
    val newfun =
      s"""
         |package ${packageName};
         |
         |${scriptCacheKey.originalCode}
         |
            """.stripMargin
    val fullClassName = packageName + "." + className

    scriptCacheKey.copy(wrappedCode = newfun, className = fullClassName)
  }

  override def invokeFunctionFromInstance(scriptCacheKey: ScriptUDFCacheKey)
  : (Seq[Object]) => AnyRef = {
    lazy val clz = JavaRuntimeCompileUDF.driverExecute(scriptCacheKey).asInstanceOf[Class[_]]
    lazy val instance = SourceCodeCompiler.newInstance(clz)
    lazy val method = SourceCodeCompiler.getMethod(clz, scriptCacheKey.methodName)

    val func: (Seq[Object]) => AnyRef = {
      (args: Seq[Object]) => method.invoke(instance, args: _*)
    }
    func
  }

  /**
    * validate the source code
    */
  override def check(sourceCode: String): Boolean = {
    true
  }

  /**
    * how to compile the language source code with jvm.
    *
    * @param scriptCacheKey
    * @return
    */
  override def compile(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
    logInfo("compile java source code: \n" + scriptCacheKey.wrappedCode)
    SourceCodeCompiler.compileJava(scriptCacheKey.wrappedCode, scriptCacheKey.className)
  }

  override def lang: String = "java"
}

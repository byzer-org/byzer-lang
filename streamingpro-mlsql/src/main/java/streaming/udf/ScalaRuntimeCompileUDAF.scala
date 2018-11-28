package streaming.udf

import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import org.python.antlr.ast.ClassDef
import streaming.common.SourceCodeCompiler
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey

/**
 * Created by fchen on 2018/11/14.
 */
object ScalaRuntimeCompileUDAF extends RuntimeCompileUDAF with ScalaCompileUtils {
  /**
   * validate the source code
   */
  override def check(sourceCode: String): Boolean = {
    val tree = tb.parse(sourceCode)
    val typeCheckResult = tb.typecheck(tree)
    val checkResult = typeCheckResult.isInstanceOf[ClassDef]
    if (!checkResult) {
      throw new IllegalArgumentException("scala udaf require a class define!")
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
    val tree = tb.parse(prepareScala(scriptCacheKey.originalCode, scriptCacheKey.className))
    tb.compile(tree).apply().asInstanceOf[Class[_]]
  }

  override def generateFunction(scriptCacheKey: ScriptUDFCacheKey): UserDefinedAggregateFunction = {
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
    new UserDefinedAggregateFunction with Serializable {

      @transient val clazzUsingInDriver = wrap(() => {
        execute(scriptCacheKey)
      }).asInstanceOf[Class[_]]
      @transient val instanceUsingInDriver = newInstance(clazzUsingInDriver)

      lazy val clazzUsingInExecutor = wrap(() => {
        execute(scriptCacheKey)
      }).asInstanceOf[Class[_]]
      lazy val instanceUsingInExecutor = newInstance(clazzUsingInExecutor)

      def invokeMethod[T: ClassTag](clazz: Class[_], instance: Any, method: String): T = {
        wrap(() => {
          SourceCodeCompiler.getMethod(clazz, method).invoke(instance)
        }).asInstanceOf[T]
      }

      val _inputSchema = invokeMethod[StructType](clazzUsingInDriver, instanceUsingInDriver, "inputSchema")
      val _dataType = invokeMethod[DataType](clazzUsingInDriver, instanceUsingInDriver, "dataType")
      val _bufferSchema = invokeMethod[StructType](clazzUsingInDriver, instanceUsingInDriver, "bufferSchema")
      val _deterministic = invokeMethod[Boolean](clazzUsingInDriver, instanceUsingInDriver, "deterministic")

      override def inputSchema: StructType = {
        _inputSchema
      }

      override def dataType: DataType = {
        _dataType
      }

      override def bufferSchema: StructType = {
        _bufferSchema
      }

      override def deterministic: Boolean = {
        _deterministic
      }

      lazy val _update = SourceCodeCompiler.getMethod(clazzUsingInExecutor, "update")
      lazy val _merge = SourceCodeCompiler.getMethod(clazzUsingInExecutor, "merge")
      lazy val _initialize = SourceCodeCompiler.getMethod(clazzUsingInExecutor, "initialize")
      lazy val _evaluate = SourceCodeCompiler.getMethod(clazzUsingInExecutor, "evaluate")

      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        wrap(() => {
          _update.invoke(instanceUsingInExecutor, buffer, input)
        })

      }

      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        wrap(() => {
          _merge.invoke(instanceUsingInExecutor, buffer1, buffer2)
        })
      }

      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        wrap(() => {
          _initialize.invoke(instanceUsingInExecutor, buffer)
        })
      }

      override def evaluate(buffer: Row): Any = {
        wrap(() => {
          _evaluate.invoke(instanceUsingInExecutor, buffer)
        })
      }

    }
  }

  override def lang: String = "scala"
}

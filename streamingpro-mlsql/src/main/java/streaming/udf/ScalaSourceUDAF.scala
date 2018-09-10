package streaming.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import streaming.common.{ScriptCacheKey, SourceCodeCompiler}
import streaming.dsl.ScriptSQLExec
import streaming.log.{Logging, WowLog}

import scala.reflect.ClassTag


object ScalaSourceUDAF extends Logging with WowLog {
  def apply(src: String, className: String): UserDefinedAggregateFunction = {
    generateAggregateFunction(src, className)
  }

  private def generateAggregateFunction(src: String, className: String): UserDefinedAggregateFunction = {
    new UserDefinedAggregateFunction with Serializable {

      val c = ScriptSQLExec.contextGetOrForTest()

      val wrap = (fn: () => Any) => {
        try {
          ScriptSQLExec.setContextIfNotPresent(c)
          fn()
        } catch {
          case e: Exception =>
            logError(format_cause(e))
            throw e
        }
      }

      @transient val clazzUsingInDriver = wrap(() => {
        SourceCodeCompiler.execute(ScriptCacheKey(src, className))
      }).asInstanceOf[Class[_]]
      @transient val instanceUsingInDriver = SourceCodeCompiler.newInstance(clazzUsingInDriver)

      lazy val clazzUsingInExecutor = wrap(() => {
        SourceCodeCompiler.execute(ScriptCacheKey(src, className))
      }).asInstanceOf[Class[_]]
      lazy val instanceUsingInExecutor = SourceCodeCompiler.newInstance(clazzUsingInExecutor)

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
}

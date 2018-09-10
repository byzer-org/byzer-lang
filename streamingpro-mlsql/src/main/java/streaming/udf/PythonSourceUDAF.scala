package streaming.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import org.python.core.{Py, PyObject}
import streaming.common.{ScriptCacheKey, SourceCodeCompiler}
import streaming.dsl.ScriptSQLExec
import streaming.jython.JythonUtils
import streaming.log.{Logging, WowLog}


/**
  * Created by allwefantasy on 31/8/2018.
  */
object PythonSourceUDAF extends Logging with WowLog {
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
            logError(format_exception(e))
            throw e
        }
      }

      @transient val objectUsingInDriver = wrap(() => {
        SourceCodeCompiler.execute(ScriptCacheKey(src, className, "python")).asInstanceOf[PyObject].__call__()
      }).asInstanceOf[PyObject]

      lazy val objectUsingInExecutor = wrap(() => {
        SourceCodeCompiler.execute(ScriptCacheKey(src, className, "python")).asInstanceOf[PyObject].__call__()
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
}

package streaming.udf

import java.util.UUID

import org.apache.spark.sql.types._
import org.python.core._
import streaming.common.{ScriptCacheKey, SourceCodeCompiler}
import streaming.dsl.ScriptSQLExec
import streaming.jython.JythonUtils
import streaming.log.{Logging, WowLog}
import streaming.parser.SparkTypePaser

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 28/8/2018.
  */
object PythonSourceUDF extends Logging with WowLog {

  private def wrapClass(function: String) = {
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

  def apply(src: String, methodName: Option[String], returnType: String): (AnyRef, DataType) = {
    val (className, newfun) = wrapClass(src)
    apply(newfun, className, methodName, returnType)
  }


  def apply(src: String, className: String, methodName: Option[String], returnType: String): (AnyRef, DataType) = {
    val argumentNum = getParameterCount(src, className, methodName)
    (generateFunction(src, className, methodName, argumentNum), SparkTypePaser.toSparkType(returnType))
  }


  private def getParameterCount(src: String, classMethod: String, methodName: Option[String]): Int = {

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

    val po = wrap(() => {
      SourceCodeCompiler.execute(ScriptCacheKey(src, classMethod, "python"))
    })
    val pi = po.asInstanceOf[PyObject].__getattr__(methodName.getOrElse("apply")).asInstanceOf[PyMethod]
    pi.__func__.asInstanceOf[PyFunction].__code__.asInstanceOf[PyTableCode].co_argcount - 1
  }

  def generateFunction(src: String, className: String, methodName: Option[String], argumentNum: Int): AnyRef = {
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

    lazy val instance = wrap(() => {
      SourceCodeCompiler.execute(ScriptCacheKey(src, className, "python")).asInstanceOf[PyObject].__call__()
    }).asInstanceOf[PyObject]
    lazy val method = instance.__getattr__(methodName.getOrElse("apply"))

    argumentNum match {
      case 0 => new Function0[Any] with Serializable {
        override def apply(): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__())
          })
        }
      }
      case 1 => new Function1[Object, Any] with Serializable {
        override def apply(v1: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(JythonUtils.toPy(v1)))
          })

        }
      }
      case 2 => new Function2[Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(JythonUtils.toPy(v1), JythonUtils.toPy(v2)))
          })

        }
      }
      case 3 => new Function3[Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3)))
          })

        }
      }
      case 4 => new Function4[Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4)))
          })

        }
      }
      case 5 => new Function5[Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5))))
          })
        }
      }
      case 6 => new Function6[Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6))))
          })

        }
      }
      case 7 => new Function7[Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7))))
          })
        }
      }
      case 8 => new Function8[Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8))))
          })
        }
      }
      case 9 => new Function9[Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9))))
          })

        }
      }
      case 10 => new Function10[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10))))
          })
        }
      }
      case 11 => new Function11[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10), JythonUtils.toPy(v11))))
          })

        }
      }
      case 12 => new Function12[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10), JythonUtils.toPy(v11), JythonUtils.toPy(v12))))
          })
        }
      }
      case 13 => new Function13[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10), JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13))))
          })

        }
      }
      case 14 => new Function14[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14))))
          })

        }
      }
      case 15 => new Function15[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object, v15: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14), JythonUtils.toPy(v15))))
          })

        }
      }
      case 16 => new Function16[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object, v15: Object, v16: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14), JythonUtils.toPy(v15), JythonUtils.toPy(v16))))
          })
        }
      }
      case 17 => new Function17[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object, v15: Object, v16: Object, v17: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14), JythonUtils.toPy(v15), JythonUtils.toPy(v16), JythonUtils.toPy(v17))))
          })
        }
      }
      case 18 => new Function18[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object, v15: Object, v16: Object, v17: Object, v18: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14), JythonUtils.toPy(v15), JythonUtils.toPy(v16), JythonUtils.toPy(v17), JythonUtils.toPy(v18))))
          })

        }
      }
      case 19 => new Function19[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object, v15: Object, v16: Object, v17: Object, v18: Object, v19: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14), JythonUtils.toPy(v15), JythonUtils.toPy(v16), JythonUtils.toPy(v17), JythonUtils.toPy(v18), JythonUtils.toPy(v19))))
          })

        }
      }
      case 20 => new Function20[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object, v15: Object, v16: Object, v17: Object, v18: Object, v19: Object, v20: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14), JythonUtils.toPy(v15),
              JythonUtils.toPy(v16), JythonUtils.toPy(v17), JythonUtils.toPy(v18), JythonUtils.toPy(v19), JythonUtils.toPy(v20))))
          })

        }
      }
      case 21 => new Function21[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object, v15: Object, v16: Object, v17: Object, v18: Object, v19: Object, v20: Object, v21: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14), JythonUtils.toPy(v15),
              JythonUtils.toPy(v16), JythonUtils.toPy(v17), JythonUtils.toPy(v18), JythonUtils.toPy(v19), JythonUtils.toPy(v20), JythonUtils.toPy(v21))))
          })

        }
      }
      case 22 => new Function22[Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Object, Any] with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object, v8: Object, v9: Object, v10: Object, v11: Object, v12: Object, v13: Object, v14: Object, v15: Object, v16: Object, v17: Object, v18: Object, v19: Object, v20: Object, v21: Object, v22: Object): Any = {
          wrap(() => {
            JythonUtils.toJava(method.__call__(Array(JythonUtils.toPy(v1), JythonUtils.toPy(v2), JythonUtils.toPy(v3), JythonUtils.toPy(v4), JythonUtils.toPy(v5), JythonUtils.toPy(v6), JythonUtils.toPy(v7), JythonUtils.toPy(v8), JythonUtils.toPy(v9), JythonUtils.toPy(v10),
              JythonUtils.toPy(v11), JythonUtils.toPy(v12), JythonUtils.toPy(v13), JythonUtils.toPy(v14), JythonUtils.toPy(v15),
              JythonUtils.toPy(v16), JythonUtils.toPy(v17), JythonUtils.toPy(v18), JythonUtils.toPy(v19), JythonUtils.toPy(v20), JythonUtils.toPy(v21), JythonUtils.toPy(v22))))
          })

        }
      }
      case n => throw new Exception(s"UDF with $n arguments is not supported ")
    }
  }
}

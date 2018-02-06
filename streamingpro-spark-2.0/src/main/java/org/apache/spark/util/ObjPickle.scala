package org.apache.spark.util

import java.io.{DataOutputStream, OutputStream}
import java.nio.charset.StandardCharsets

import net.razorvine.pickle.{Pickler, Unpickler}
import org.apache.spark.SparkException
import org.apache.spark.input.PortableDataStream
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by allwefantasy on 6/2/2018.
  */
object ObjPickle {
  def toJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (row: InternalRow, struct: StructType) =>
      val values = new Array[Any](row.numFields)
      var i = 0
      while (i < row.numFields) {
        values(i) = toJava(row.get(i, struct.fields(i).dataType), struct.fields(i).dataType)
        i += 1
      }
      new GenericRowWithSchema(values, struct)

    case (a: ArrayData, array: ArrayType) =>
      val values = new java.util.ArrayList[Any](a.numElements())
      a.foreach(array.elementType, (_, e) => {
        values.add(toJava(e, array.elementType))
      })
      values

    case (map: MapData, mt: MapType) =>
      val jmap = new java.util.HashMap[Any, Any](map.numElements())
      map.foreach(mt.keyType, mt.valueType, (k, v) => {
        jmap.put(toJava(k, mt.keyType), toJava(v, mt.valueType))
      })
      jmap

    case (ud, udt: UserDefinedType[_]) => toJava(ud, udt.sqlType)

    case (d: Decimal, _) => d.toJavaBigDecimal

    case (s: UTF8String, StringType) => s.toString

    case (other, _) => other
  }

  def pickle(iter: Iterator[Row], schema: DataType) = {
    EvaluatePython.registerPicklers()
    val toJava: (Any) => Any = { item =>
      EvaluatePython.toJava(item, schema)
    }
    val pickle = new Pickler()
    val iterator = iter.map { f =>
      InternalRow.fromSeq(
        f.schema.fieldNames.zipWithIndex.map {
          fieldAndindex =>
            val (field, index) = fieldAndindex
            val v = f.getValuesMap(f.schema.fieldNames)(field).asInstanceOf[Any]
            f.schema(index).dataType match {
              case sv: VectorUDT =>
                sv.serialize(v.asInstanceOf[org.apache.spark.ml.linalg.Vector])
              case _ => v
            }

        }.toSeq
      )
    }.map(toJava).map { f =>
      pickle.dumps(f)
    }
    iterator
  }

  def pickleInternalRow(iter: Iterator[InternalRow], schema: DataType) = {
    EvaluatePython.registerPicklers()
    val toJava: (Any) => Any = { item =>
      EvaluatePython.toJava(item, schema)
    }
    val pickle = new Pickler()
    val iterator = iter.map(toJava).map { f =>
      pickle.dumps(f)
    }
    iterator
  }


  def unpickle(item: Array[Byte]) = {
    EvaluatePython.registerPicklers()
    val pickle = new Unpickler()
    val obj = pickle.loads(item)
    pickle.close()
    obj
  }

  def pickle(item: Any, out: OutputStream) = {
    EvaluatePython.registerPicklers()
    val pickle = new Pickler()
    pickle.dump(item, out)
    pickle.close()
  }

  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream) {

    def write(obj: Any): Unit = obj match {
      case null =>
        dataOut.writeInt(SpecialLengths.NULL)
      case arr: Array[Byte] =>
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case str: String =>
        writeUTF(str, dataOut)
      case stream: PortableDataStream =>
        write(stream.toArray())
      case (key, value) =>
        write(key)
        write(value)
      case other =>
        throw new SparkException("Unexpected element type " + other.getClass)
    }

    iter.foreach(write)
  }

  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  private object SpecialLengths {
    val END_OF_DATA_SECTION = -1
    val PYTHON_EXCEPTION_THROWN = -2
    val TIMING_DATA = -3
    val END_OF_STREAM = -4
    val NULL = -5
  }

}

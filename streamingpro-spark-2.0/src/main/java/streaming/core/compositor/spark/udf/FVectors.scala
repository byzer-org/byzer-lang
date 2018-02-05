package streaming.core.compositor.spark.udf

import org.apache.spark.SparkException
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}

import scala.collection.mutable.ArrayBuilder

/**
  * Created by allwefantasy on 22/1/2018.
  */
object FVectors {
  def assemble(vv: Any*): Vector = {
    val indices = ArrayBuilder.make[Int]
    val values = ArrayBuilder.make[Double]
    var cur = 0
    vv.foreach {
      case v: Double =>
        if (v != 0.0) {
          indices += cur
          values += v
        }
        cur += 1
      case vec: Vector =>
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += cur + i
            values += v
          }
        }
        cur += vec.size
      case null =>
        // TODO: output Double.NaN?
        throw new SparkException("Values to assemble cannot be null.")
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
    }
    Vectors.sparse(cur, indices.result(), values.result()).compressed
  }

  def slice(vector: SparseVector, selectedIndices: Array[Int]): SparseVector = {
    val values = vector.values
    var currentIdx = 0
    val (sliceInds, sliceVals) = selectedIndices.flatMap { origIdx =>
      val iIdx = java.util.Arrays.binarySearch(selectedIndices, origIdx)
      val i_v = if (iIdx >= 0) {
        Iterator((currentIdx, values(iIdx)))
      } else {
        Iterator()
      }
      currentIdx += 1
      i_v
    }.unzip
    new SparseVector(selectedIndices.length, sliceInds.toArray, sliceVals.toArray)
  }
}

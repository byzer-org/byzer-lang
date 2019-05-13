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

  def range(vector: SparseVector, range: Array[Int]): SparseVector = {
    val start = range(0)
    val end = range(1)

    val (sliceVals, sliceInds) = vector.values.zip(vector.indices).filter { d =>
      val (v, i) = d
      i < end && i >= start
    }.unzip
    new SparseVector(end - start, sliceInds.toArray, sliceVals.toArray)
  }
}
